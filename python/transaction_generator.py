#!/usr/bin/env python3
import json
import time
import random
import numpy as np
import redis
import signal
import sys
from datetime import datetime
from kafka import KafkaProducer
import logging
import math
from typing import Dict, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
KAFKA_TOPIC = "Transactions"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:29092"]
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Transaction parameters
MIN_TRANSACTION_VALUE = 1.0
MAX_TRANSACTION_VALUE = 5000.0

# World coordinates bounds (approximate)
LAT_BOUNDS = (-90.0, 90.0)
LON_BOUNDS = (-180.0, 180.0)

# Common transaction locations (major cities around the world)
COMMON_LOCATIONS = [
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"name": "Sydney", "lat": -33.8688, "lon": 151.2093},
    {"name": "Warsaw", "lat": 52.2297, "lon": 21.0122},
    {"name": "Berlin", "lat": 52.5200, "lon": 13.4050},
    {"name": "Madrid", "lat": 40.4168, "lon": -3.7038},
    {"name": "Rome", "lat": 41.9028, "lon": 12.4964},
    {"name": "Toronto", "lat": 43.6532, "lon": -79.3832}
]

# Anomaly probabilities
ANOMALY_PROBABILITY = 0.02  # 5% chance of an anomaly
ANOMALY_TYPES = {
    # "large_distance": 0.1,           # Large distance from previous transaction
    "high_value": 0.2,               # Value 10x higher than average
    "very_high_value": 0.2,          # Value > 10000 PLN
    "rapid_transactions": 0.1,       # Transactions < 10s apart
    "negative_transaction": 0.05,    # Negative transaction value
    "impossible_travel": 0.1,        # Transactions too far apart given time difference
    # "limit_exceeded": 0.1,           # Exceeding card limit
    "pin_avoidance": 0.1,            # Multiple transactions just below PIN threshold (90-100 PLN)
    "pin_avoidance2": 0.05,            # Multiple transactions just below PIN threshold (90-100 PLN)
    "multi_card_distance": 0.1,      # Same user, different cards, large distance, small time
    "dormant_card_activity": 0.1    # Sudden activity after long dormancy
}


class TransactionGenerator:
    def __init__(self):
        """Initialize the transaction generator"""
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.running = True
        self.cards = {}
        self.users = {}
        self.last_transactions = {}  # Store last transaction time and location per card
        self.user_last_transactions = {}  # Store last transaction per user across all cards
        self.inactive_cards = set()  # Store IDs of inactive cards

        # Register signal handlers
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        # Load data from Redis
        self.load_data()

    def load_data(self):
        """Load card and user data from Redis"""
        if not self.redis_client.exists("card_count"):
            logger.error("No data found in Redis. Please run initialize_data.py first.")
            sys.exit(1)

        logger.info("Loading card and user data from Redis...")
        card_count = int(self.redis_client.get("card_count"))

        # Load card data
        for card_id in range(1, card_count + 1):
            card_data = self.redis_client.hgetall(f"card:{card_id}")
            if card_data:
                # Convert numeric fields
                for field in ["id", "user_id", "transaction_count"]:
                    if field in card_data:
                        card_data[field] = int(card_data[field])
                for field in ["avg_value", "std_dev", "limit", "available_limit", "last_lat", "last_lon", "last_transaction_time"]:
                    if field in card_data:
                        card_data[field] = float(card_data[field])

                self.cards[card_id] = card_data
                self.last_transactions[card_id] = {
                    "time": card_data.get("last_transaction_time", 0),
                    "lat": card_data.get("last_lat", 0),
                    "lon": card_data.get("last_lon", 0)
                }
                current_time = datetime.now().timestamp()
                last_transaction_time = card_data.get("last_transaction_time", 0)
                days_since_last_transaction = (current_time - last_transaction_time) / (86400)

                if days_since_last_transaction >= 30:
                    self.inactive_cards.add(card_id)
                    logger.info(f"Card {card_id} marked as inactive (no transactions for {days_since_last_transaction:.1f} days)")

                # Initialize user's last transaction if this is more recent
                user_id = card_data["user_id"]
                if user_id not in self.user_last_transactions:
                    self.user_last_transactions[user_id] = {
                        "time": card_data.get("last_transaction_time", 0),
                        "lat": card_data.get("last_lat", 0),
                        "lon": card_data.get("last_lon", 0),
                        "card_id": card_id
                    }
                elif card_data.get("last_transaction_time", 0) > self.user_last_transactions[user_id]["time"]:
                    self.user_last_transactions[user_id] = {
                        "time": card_data.get("last_transaction_time", 0),
                        "lat": card_data.get("last_lat", 0),
                        "lon": card_data.get("last_lon", 0),
                        "card_id": card_id
                    }

        # Load user data
        user_count = int(self.redis_client.get("user_count") or card_count)  # Fallback to card_count if user_count not set
        for user_id in range(1, user_count + 1):
            user_data = self.redis_client.hgetall(f"user:{user_id}")
            if user_data:
                if "cards" in user_data:
                    user_data["cards"] = json.loads(user_data["cards"])
                if "id" in user_data:
                    user_data["id"] = int(user_data["id"])
                self.users[user_id] = user_data

                # Ensure every user has an entry in user_last_transactions
                if user_id not in self.user_last_transactions:
                    self.user_last_transactions[user_id] = {
                        "time": 0,
                        "lat": 0,
                        "lon": 0,
                        "card_id": None
                    }

        logger.info(f"Loaded {len(self.cards)} cards and {len(self.users)} users from Redis")

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points in kilometers using Haversine formula"""
        R = 6371  # Earth radius in kilometers

        # Convert to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)

        # Haversine formula
        dlon = lon2_rad - lon1_rad
        dlat = lat2_rad - lat1_rad
        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        distance = R * c

        return distance

    def generate_location(self, last_lat: float, last_lon: float, is_anomaly: bool = False) -> Tuple[float, float]:
        """Generate a new location based on the last known location"""
        if is_anomaly:
            # For anomalies, we might want to generate a location far away
            if random.random() < 0.7:  # 70% chance to use a common location
                location = random.choice(COMMON_LOCATIONS)
                return location["lat"], location["lon"]
            else:  # 30% chance for a completely random location
                return random.uniform(*LAT_BOUNDS), random.uniform(*LON_BOUNDS)
        else:
            # For normal transactions, we want to stay close to the last location
            # Small movement (within ~10km)
            lat_variation = random.normalvariate(0, 0.05)  # ~5km in latitude
            lon_variation = random.normalvariate(0, 0.05)  # ~5km in longitude

            new_lat = last_lat + lat_variation
            new_lon = last_lon + lon_variation

            # Ensure within world bounds
            new_lat = max(LAT_BOUNDS[0], min(new_lat, LAT_BOUNDS[1]))
            new_lon = max(LON_BOUNDS[0], min(new_lon, LON_BOUNDS[1]))

            return new_lat, new_lon

    def generate_normal_transaction(self, card_id: int) -> Dict:
        """Generate a normal transaction for a card"""
        card = self.cards[card_id]

        # Generate transaction value based on card's average and std dev
        value = np.random.normal(card["avg_value"], card["std_dev"])
        value = max(MIN_TRANSACTION_VALUE, min(value, MAX_TRANSACTION_VALUE))
        value = round(value, 2)

        # Generate location near last known location
        last_lat = card["last_lat"]
        last_lon = card["last_lon"]

        lat, lon = self.generate_location(last_lat, last_lon)

        # Generate timestamp in the future using normal distribution
        current_time = datetime.now().timestamp()
        last_transaction_time = self.last_transactions[card_id]["time"]

        # If last transaction was too long ago, use current time as reference
        if current_time - last_transaction_time > 86400:  # More than a day
            reference_time = current_time
        else:
            reference_time = last_transaction_time

        # Generate time in the future with normal distribution (mean: 1 hour, std dev: 15 minutes)
        time_delta = abs(np.random.normal(3600, 900))
        timestamp = reference_time + time_delta

        transaction = {
            "card_id": card_id,
            "user_id": card["user_id"],
            "value": value,
            "latitude": lat,
            "longitude": lon,
            "timestamp": timestamp,
            "available_limit": card["available_limit"] - value,
            "anomaly": False,
            "anomaly_type": None
        }

        return transaction

    def generate_anomaly_transaction(self, card_id: int) -> Dict:
        """Generate an anomalous transaction based on randomly selected anomaly type"""
        # Get list of possible anomaly types for this card
        possible_anomalies = list(ANOMALY_TYPES.keys())
        anomaly_weights = list(ANOMALY_TYPES.values())
        current_time = datetime.now().timestamp()
        last_transaction_time = self.last_transactions[card_id]["time"]

        # Check if card is inactive based on Redis data
        is_inactive = card_id in self.inactive_cards

        # If card is not inactive, remove dormant_card_activity from possible anomalies
        if not is_inactive:
            if "dormant_card_activity" in possible_anomalies:
                idx = possible_anomalies.index("dormant_card_activity")
                possible_anomalies.pop(idx)
                anomaly_weights.pop(idx)
        else:
            # If card is inactive, increase probability of dormant_card_activity
            if is_inactive:
                possible_anomalies = ["dormant_card_activity"]
                anomaly_weights = [1.0]

        # Select anomaly type based on adjusted weights
        anomaly_type = random.choices(
            possible_anomalies,
            weights=anomaly_weights,
            k=1
        )[0]

        card = self.cards[card_id]
        transaction = self.generate_normal_transaction(card_id)
        transaction["anomaly"] = True
        transaction["anomaly_type"] = anomaly_type

        # Modify transaction based on anomaly type
        if anomaly_type == "large_distance":
            # Generate location far away (>1000km)
            transaction["latitude"], transaction["longitude"] = self.generate_location(card["last_lat"], card["last_lon"], is_anomaly=True)
            # Keep current timestamp
        elif anomaly_type == "high_value":
            transaction["value"] = card["avg_value"] * 10
            # Keep current timestamp
        elif anomaly_type == "very_high_value":
            transaction["value"] = random.uniform(10000, 20000)
            # Keep current timestamp
        elif anomaly_type == "rapid_transactions":
            # Generate two transactions in quick succession
            # First send a normal transaction
            normal_tx = self.generate_normal_transaction(card_id)
            normal_tx["timestamp"] = transaction["timestamp"]
            self.producer.send(KAFKA_TOPIC, normal_tx)
            self.update_local_cache(card_id, normal_tx)
            # Then this anomalous one shortly after (in the future)
            transaction["timestamp"] = transaction["timestamp"] + random.uniform(1, 10)  # 1-10 seconds later
        elif anomaly_type == "negative_transaction":
            transaction["value"] = -random.uniform(1, 100)
            # Keep current timestamp
        elif anomaly_type == "impossible_travel":
            # if days_since_last_transaction < 1:
            transaction["latitude"], transaction["longitude"] = self.generate_location(card["last_lat"], card["last_lon"], is_anomaly=True)
            transaction["timestamp"] = last_transaction_time + random.uniform(1, 10)
            # else:
            #     # Not a good candidate for impossible travel, try another anomaly
            #     return self.generate_anomaly_transaction(card_id)
        elif anomaly_type == "limit_exceeded":
            transaction["value"] = card["available_limit"] + random.uniform(1, 100)
            transaction["available_limit"] -= transaction["value"]
            # Keep current timestamp
        elif anomaly_type == "pin_avoidance":
            # Generate 2-3 transactions just below PIN threshold
            num_transactions = random.randint(2, 3)
            current_time = transaction["timestamp"]
            for i in range(num_transactions - 1):
                pin_tx = self.generate_normal_transaction(card_id)
                pin_tx["value"] = random.uniform(90, 100)
                pin_tx["anomaly"] = True
                pin_tx["anomaly_type"] = "pin_avoidance"
                pin_tx["timestamp"] = current_time
                current_time += random.uniform(60, 300)  # 1-5 minutes later
                self.producer.send(KAFKA_TOPIC, pin_tx)
                self.update_local_cache(card_id, pin_tx)
                time.sleep(0.001)  # Small delay between sending

            transaction["value"] = random.uniform(90, 100)
            transaction["timestamp"] = current_time  # Set to the latest time
        elif anomaly_type == "pin_avoidance2":
            # Generate 2-3 transactions just below PIN threshold
            num_transactions = random.randint(5, 6)
            current_time = transaction["timestamp"]
            for i in range(num_transactions - 1):
                pin_tx = self.generate_normal_transaction(card_id)
                pin_tx["value"] = random.uniform(90, 100)
                pin_tx["anomaly"] = True
                pin_tx["anomaly_type"] = "pin_avoidance2"
                pin_tx["timestamp"] = current_time
                current_time += random.uniform(60, 300)  # 1-5 minutes later
                self.producer.send(KAFKA_TOPIC, pin_tx)
                self.update_local_cache(card_id, pin_tx)
                time.sleep(0.001)  # Small delay between sending

            transaction["value"] = random.uniform(90, 100)
            transaction["timestamp"] = current_time  # Set to the latest time
        elif anomaly_type == "multi_card_distance":
            # Check if user has multiple cards
            user_id = card["user_id"]
            user_cards = self.users[user_id]["cards"]

            if len(user_cards) > 1:
                # Get user's last transaction data
                user_last_tx = self.user_last_transactions.get(user_id)

                if user_last_tx and user_last_tx["card_id"] != card_id:
                    # Use a different card than the last one used
                    last_lat = user_last_tx["lat"]
                    last_lon = user_last_tx["lon"]
                    last_time = user_last_tx["time"]

                    # Generate a location far from the last transaction
                    transaction["latitude"], transaction["longitude"] = self.generate_location(
                        last_lat, last_lon, is_anomaly=True
                    )

                    # Set timestamp to be very close to the last transaction
                    # This makes it physically impossible to be in both places
                    time_diff = random.uniform(60, 300)  # 1-5 minutes
                    transaction["timestamp"] = last_time + time_diff

                    # Calculate the distance between the two locations
                    distance = self.calculate_distance(
                        last_lat, last_lon,
                        transaction["latitude"], transaction["longitude"]
                    )

                    # Calculate the minimum time needed to travel this distance (assuming 900 km/h by plane)
                    min_travel_time = (distance / 900) * 3600  # hours to seconds

                    # If the time difference is less than the minimum travel time, it's an anomaly
                    if time_diff < min_travel_time:
                        logger.info(f"Generated an anomalous transaction for user {transaction['user_id']} with cards {transaction['card_id']} at {transaction['timestamp']} (type: {anomaly_type})")
                        return transaction
            return self.generate_anomaly_transaction(card_id)
        elif anomaly_type == "dormant_card_activity":
            # This card is already dormant (checked above)
            # Generate 2-3 transactions in quick succession after long dormancy
            num_transactions = random.randint(2, 3)
            current_time = transaction["timestamp"]
            for i in range(num_transactions - 1):
                dormant_tx = self.generate_normal_transaction(card_id)
                dormant_tx["anomaly"] = True
                dormant_tx["anomaly_type"] = "dormant_card_activity"
                dormant_tx["timestamp"] = current_time
                current_time += random.uniform(300, 3600)  # 5-60 minutes later
                self.producer.send(KAFKA_TOPIC, dormant_tx)
                self.update_local_cache(card_id, dormant_tx)
                time.sleep(0.001)
            if card_id in self.inactive_cards:
                self.inactive_cards.remove(card_id)
            transaction["timestamp"] = current_time  # Set to the latest time
        return transaction

    def generate_transaction(self):
        """Generate a single transaction (normal or anomalous)"""
        # Select a random card
        card_id = random.randint(1, len(self.cards))
        if self.cards[card_id]["available_limit"] < 0:
            return self.generate_transaction()

        # Decide if this will be an anomalous transaction
        is_anomaly = random.random() < ANOMALY_PROBABILITY

        if is_anomaly:
            transaction = self.generate_anomaly_transaction(card_id)
            if transaction["anomaly_type"] == "multi_card_distance" or transaction["anomaly_type"] == "impossible_travel":
                #self.update_local_cache(card_id, transaction)
                self.producer.send(KAFKA_TOPIC, transaction)
                transaction = self.generate_normal_transaction(card_id)
                return transaction
        else:
            if card_id in self.inactive_cards:
                return self.generate_transaction()
            transaction = self.generate_normal_transaction(card_id)

        # Update local cache for future transactions
        self.update_local_cache(card_id, transaction)
        return transaction

    def update_local_cache(self, card_id, transaction):
        """Update local cache with transaction data (not Redis)"""
        # Update last transaction time and location in local cache
        self.last_transactions[card_id] = {
            "time": transaction["timestamp"],
            "lat": transaction["latitude"],
            "lon": transaction["longitude"]
        }

        # Update card data in local cache
        self.cards[card_id]["last_lat"] = transaction["latitude"]
        self.cards[card_id]["last_lon"] = transaction["longitude"]
        self.cards[card_id]["last_transaction_time"] = transaction["timestamp"]
        self.cards[card_id]["available_limit"] = transaction["available_limit"]
        self.cards[card_id]["transaction_count"] += 1

        # Update user's last transaction data
        user_id = transaction["user_id"]
        if user_id not in self.user_last_transactions or transaction["timestamp"] > self.user_last_transactions[user_id]["time"]:
            self.user_last_transactions[user_id] = {
                "time": transaction["timestamp"],
                "lat": transaction["latitude"],
                "lon": transaction["longitude"],
                "card_id": card_id
            }

    def run(self):
        """Run the transaction generator"""
        logger.info("Starting transaction generator...")

        try:
            while self.running:
                # Generate a transaction
                transaction = self.generate_transaction()

                # Send to Kafka
                if transaction:
                    self.producer.send(KAFKA_TOPIC, transaction)

                # Log transaction details
                # if transaction["anomaly"]:
                # logger.info(f"Generated anomalous transaction: {transaction['anomaly_type']} for card {transaction['card_id']}")
                # else:
                # logger.debug(f"Generated normal transaction for card {transaction['card_id']}")

                time.sleep(0.1)

        except Exception as e:
            logger.error(f"Error in transaction generator: {e}")
            self.handle_shutdown(None, None)

    def handle_shutdown(self, sig, frame):
        """Handle shutdown signals"""
        logger.info("Shutting down transaction generator...")
        self.running = False
        self.producer.flush()
        self.producer.close()
        sys.exit(0)


if __name__ == "__main__":
    generator = TransactionGenerator()
    generator.run()
