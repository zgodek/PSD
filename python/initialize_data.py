#!/usr/bin/env python3
import json
import random
import redis
import logging
from datetime import datetime, timedelta
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# World coordinates bounds
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


def initialize_data(num_users, num_cards, force=False):
    """Initialize card and user data in Redis"""
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Check if data already exists
    if redis_client.exists("card_count") and not force:
        logger.warning("Data already exists in Redis. Use --force to reinitialize.")
        return False

    logger.info(f"Initializing data with {num_users} users and {num_cards} cards...")

    # Clear existing data if force is True
    if force:
        logger.info("Clearing existing data...")
        for key in redis_client.keys("card:*"):
            redis_client.delete(key)
        for key in redis_client.keys("user:*"):
            redis_client.delete(key)
        redis_client.delete("card_count")

    # Create users with their locations
    user_locations = {}
    for user_id in range(1, num_users + 1):
        # Generate random location for user
        if random.random() < 0.8:
            location = random.choice(COMMON_LOCATIONS)
            lat, lon = location["lat"], location["lon"]
        else:
            lat = random.uniform(*LAT_BOUNDS)
            lon = random.uniform(*LON_BOUNDS)

        user_locations[user_id] = (lat, lon)

        user_key = f"user:{user_id}"
        user_data = {
            "id": user_id,
            "cards": "[]",
            "last_lat": lat,
            "last_lon": lon,
        }
        redis_client.hset(user_key, mapping=user_data)
        logger.debug(f"Created user {user_id}")

    # Create cards and assign to users
    for card_id in range(1, num_cards + 1):
        user_id = random.randint(1, num_users)

        avg_value = random.uniform(10, 500)
        std_dev = avg_value * 0.2
        card_limit = random.uniform(5000, 500000)

        lat, lon = user_locations[user_id]

        card_key = f"card:{card_id}"
        card_data = {
            "id": card_id,
            "user_id": user_id,
            "avg_value": avg_value,
            "std_dev": std_dev,
            "limit": card_limit,
            "available_limit": card_limit,
            "last_lat": lat,
            "last_lon": lon,
            "last_transaction_time": (datetime.now() - timedelta(days=random.randint(0, 30))).timestamp(),
            "transaction_count": 0
        }

        # Store in Redis
        redis_client.hset(card_key, mapping=card_data)

        # Update user's cards list
        user_cards = redis_client.hget(f"user:{user_id}", "cards") or "[]"
        user_cards_list = json.loads(user_cards)
        user_cards_list.append(card_id)
        redis_client.hset(f"user:{user_id}", "cards", json.dumps(user_cards_list))
        redis_client.hset(f"processor_user:{user_id}", "cards", json.dumps(user_cards_list))

        logger.debug(f"Created card {card_id} for user {user_id}")

    redis_client.set("card_count", num_cards)
    redis_client.set("user_count", num_users)
    logger.info(f"Successfully initialized {num_cards} cards for {num_users} users")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Initialize credit card transaction data in Redis')
    parser.add_argument('--users', type=int, default=2, help='Number of users to create')
    parser.add_argument('--cards', type=int, default=4, help='Number of cards to create')
    parser.add_argument('--force', action='store_true', help='Force reinitialization even if data exists')
    args = parser.parse_args()

    initialize_data(args.users, args.cards, args.force)
