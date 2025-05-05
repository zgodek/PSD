#!/usr/bin/env python3
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.gridspec import GridSpec
import numpy as np
import redis
import threading
import collections
from datetime import datetime
from kafka import KafkaConsumer
import logging
import argparse
from typing import List, Optional
import folium
from folium.plugins import HeatMap
import time
import sys
import matplotlib.dates as mdates

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
KAFKA_TOPIC = "Transactions"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:29092"]
REDIS_HOST = "localhost"
REDIS_PORT = 6379
MAX_POINTS = 10000  # Maximum number of points to display


class TransactionVisualizer:
    def __init__(self, card_id: Optional[int] = None, user_id: Optional[int] = None):
        """Initialize the transaction visualizer for a specific card or user"""
        self.card_id = card_id
        self.user_id = user_id

        if card_id is None and user_id is None:
            raise ValueError("Either card_id or user_id must be provided")

        # Connect to Redis
        try:
            logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
            self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            self.redis_client.ping()  # Test connection
            logger.info("Connected to Redis successfully")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            sys.exit(1)

        # Initialize Kafka consumer
        try:
            logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000  # 10 seconds timeout
            )
            # Test connection by getting topics
            topics = self.consumer.topics()
            logger.info(f"Connected to Kafka successfully. Available topics: {topics}")

            if KAFKA_TOPIC not in topics:
                logger.warning(f"Topic '{KAFKA_TOPIC}' does not exist. Make sure it's created.")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            sys.exit(1)

        # Data structures for storing transaction data
        self.transactions = collections.defaultdict(list)  # card_id -> list of transactions
        self.anomalies = collections.defaultdict(list)     # card_id -> list of anomalous transactions
        self.data_lock = threading.Lock()

        # If user_id is provided, get all cards for this user
        self.user_cards = []
        if self.user_id is not None:
            try:
                user_data = self.redis_client.hgetall(f"user:{self.user_id}")
                if not user_data:
                    logger.error(f"User {self.user_id} not found in Redis")
                    sys.exit(1)

                if "cards" in user_data:
                    self.user_cards = json.loads(user_data["cards"])
                    if not self.user_cards:
                        logger.warning(f"User {self.user_id} has no cards")
                else:
                    logger.error(f"User {self.user_id} has no 'cards' field in Redis")
                    sys.exit(1)

                logger.info(f"Monitoring transactions for user {self.user_id} with cards: {self.user_cards}")
            except Exception as e:
                logger.error(f"Error getting user data from Redis: {e}")
                sys.exit(1)
        else:
            # Check if card exists
            try:
                card_data = self.redis_client.hgetall(f"card:{self.card_id}")
                if not card_data:
                    logger.error(f"Card {self.card_id} not found in Redis")
                    sys.exit(1)
                logger.info(f"Monitoring transactions for card {self.card_id}")
            except Exception as e:
                logger.error(f"Error getting card data from Redis: {e}")
                sys.exit(1)

        self.setup_plot()

        self.running = True
        self.consumer_thread = threading.Thread(target=self.consume_transactions, daemon=True)
        self.consumer_thread.start()

        self.map_thread = threading.Thread(target=self.update_map_periodically, daemon=True)
        self.map_thread.start()

    def setup_plot(self):
        """Set up the matplotlib plot"""
        self.fig = plt.figure(figsize=(14, 10))
        gs = GridSpec(3, 2, figure=self.fig)

        # Transaction value over time
        self.ax_value = self.fig.add_subplot(gs[0, :])
        self.ax_value.set_title('Wartości transakcji w czasie')
        self.ax_value.set_xlabel('Czas')
        self.ax_value.set_ylabel('Wartość (PLN)')
        self.ax_value.grid(True)

        # Available limit over time
        self.ax_limit = self.fig.add_subplot(gs[1, 0])
        self.ax_limit.set_title('Dostępny limit w czasie')
        self.ax_limit.set_xlabel('Czas')
        self.ax_limit.set_ylabel('Dostępny limit (PLN)')
        self.ax_limit.grid(True)

        # Transaction frequency by hour
        self.ax_freq = self.fig.add_subplot(gs[1, 1])
        self.ax_freq.set_title('Częstotliwość transakcji wg godziny')
        self.ax_freq.set_xlabel('Godzina dnia')
        self.ax_freq.set_ylabel('Liczba transakcji')
        self.ax_freq.grid(True)

        # Anomaly alerts over time
        self.ax_anomaly = self.fig.add_subplot(gs[2, :])
        self.ax_anomaly.set_title('Alarmy anomalii w czasie')
        self.ax_anomaly.set_xlabel('Czas')
        self.ax_anomaly.set_ylabel('Status alarmu')
        self.ax_anomaly.grid(True)
        self.ax_anomaly.set_yticks([0, 1])
        self.ax_anomaly.set_yticklabels(['Brak alarmu', 'Alarm'])

        plt.tight_layout()

        # Set up animation
        self.ani = animation.FuncAnimation(
            self.fig, self.update_plot, interval=1000, cache_frame_data=False
        )

    def consume_transactions(self):
        """Consume transactions from Kafka"""
        logger.info("Starting to consume transactions from Kafka...")
        received_count = 0

        try:
            for message in self.consumer:
                if not self.running:
                    break

                transaction = message.value
                card_id = transaction["card_id"]
                received_count += 1

                if received_count % 1000 == 0:
                    logger.info(f"Received {received_count} transactions so far")

                # Check if this transaction is relevant to our visualization
                is_relevant = False
                if self.card_id is not None and card_id == self.card_id:
                    is_relevant = True
                elif self.user_id is not None and card_id in self.user_cards:
                    is_relevant = True

                if is_relevant:
                    with self.data_lock:
                        # Add transaction to our data
                        transaction_data = {
                            "timestamp": transaction["timestamp"],
                            "datetime": datetime.fromtimestamp(transaction["timestamp"]),
                            "value": transaction["value"],
                            "latitude": transaction["latitude"],
                            "longitude": transaction["longitude"],
                            "available_limit": transaction["available_limit"],
                            "anomaly": transaction["anomaly"],
                            "anomaly_type": transaction["anomaly_type"]
                        }

                        self.transactions[card_id].append(transaction_data)

                        # If anomalous, add to anomalies list
                        if transaction["anomaly"]:
                            self.anomalies[card_id].append(transaction_data)
                            logger.info(f"Detected anomaly: {transaction['anomaly_type']} for card {card_id}")

                        # Limit the number of points
                        if len(self.transactions[card_id]) > MAX_POINTS:
                            self.transactions[card_id] = self.transactions[card_id][-MAX_POINTS:]
                        if len(self.anomalies[card_id]) > MAX_POINTS:
                            self.anomalies[card_id] = self.anomalies[card_id][-MAX_POINTS:]

                    logger.debug(f"Processed transaction for card {card_id}: {transaction}")

        except Exception as e:
            logger.error(f"Error consuming transactions: {e}")
            if not self.running:
                return

            # Try to reconnect
            logger.info("Attempting to reconnect to Kafka...")
            try:
                self.consumer.close()
                self.consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    auto_offset_reset='latest',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info("Reconnected to Kafka successfully")
                self.consume_transactions()  # Restart consumption
            except Exception as e:
                logger.error(f"Failed to reconnect to Kafka: {e}")

    def update_plot(self, frame):
        """Update the matplotlib plot with new data"""
        with self.data_lock:
            # Clear all axes
            self.ax_value.clear()
            self.ax_limit.clear()
            self.ax_freq.clear()
            self.ax_anomaly.clear()

            # Set titles and labels again
            self.ax_value.set_title('Wartości transakcji w czasie')
            self.ax_value.set_xlabel('Czas')
            self.ax_value.set_ylabel('Wartość (PLN)')
            self.ax_value.grid(True)

            self.ax_limit.set_title('Dostępny limit w czasie')
            self.ax_limit.set_xlabel('Czas')
            self.ax_limit.set_ylabel('Dostępny limit (PLN)')
            self.ax_limit.grid(True)

            self.ax_freq.set_title('Częstotliwość transakcji wg godziny')
            self.ax_freq.set_xlabel('Godzina dnia')
            self.ax_freq.set_ylabel('Liczba transakcji')
            self.ax_freq.grid(True)

            self.ax_anomaly.set_title('Alarmy anomalii w czasie')
            self.ax_anomaly.set_xlabel('Czas')
            self.ax_anomaly.set_ylabel('Status alarmu')
            self.ax_anomaly.grid(True)
            self.ax_anomaly.set_yticks([0, 1])
            self.ax_anomaly.set_yticklabels(['Brak alarmu', 'Alarm'])

            # Plot data for each card
            colors = plt.cm.tab10.colors
            card_ids = list(self.transactions.keys())

            # Transaction values over time
            for i, card_id in enumerate(card_ids):
                transactions = self.transactions[card_id]
                if not transactions:
                    continue

                color = colors[i % len(colors)]

                # Extract data
                datetimes = [t["datetime"] for t in transactions]
                values = [t["value"] for t in transactions]
                anomalies = [t["anomaly"] for t in transactions]

                # Plot normal transactions
                normal_indices = [i for i, a in enumerate(anomalies) if not a]
                if normal_indices:
                    self.ax_value.plot(
                        [datetimes[i] for i in normal_indices],
                        [values[i] for i in normal_indices],
                        'o-', color=color, markersize=4, alpha=0.7,
                        label=f'Karta {card_id} (normalne)'
                    )

                # Plot anomalous transactions
                anomaly_indices = [i for i, a in enumerate(anomalies) if a]
                if anomaly_indices:
                    self.ax_value.plot(
                        [datetimes[i] for i in anomaly_indices],
                        [values[i] for i in anomaly_indices],
                        'rx', markersize=8,
                        label=f'Karta {card_id} (anomalie)'
                    )

                # Available limit over time
                limits = [t["available_limit"] for t in transactions]
                self.ax_limit.plot(
                    datetimes, limits, '-', color=color,
                    label=f'Karta {card_id}'
                )

                # Transaction frequency by hour
                hours = [d.hour for d in datetimes]
                hour_counts = np.zeros(24)
                for hour in hours:
                    hour_counts[hour] += 1

                self.ax_freq.bar(
                    np.arange(24) + i*0.2, hour_counts,
                    width=0.2, color=color, alpha=0.7,
                    label=f'Karta {card_id}'
                )

                # Plot anomaly alerts over time (0 for normal, 1 for anomaly)
                anomaly_values = [1 if a else 0 for a in anomalies]

                # Use stem plot for anomaly alerts
                markerline, stemlines, baseline = self.ax_anomaly.stem(
                    datetimes, anomaly_values,
                    linefmt=f'C{i}-', markerfmt=f'C{i}o',
                    basefmt='k-', label=f'Karta {card_id}'
                )
                plt.setp(markerline, markersize=8)
                plt.setp(stemlines, linewidth=2, alpha=0.7)

                # Add anomaly type as annotation for anomalies
                for j, is_anomaly in enumerate(anomalies):
                    if is_anomaly:
                        self.ax_anomaly.annotate(
                            transactions[j]["anomaly_type"],
                            (datetimes[j], 1.05),
                            rotation=45,
                            ha='right',
                            fontsize=8
                        )

            # Add legends
            self.ax_value.legend()
            self.ax_limit.legend()
            self.ax_freq.legend()
            self.ax_anomaly.legend()

            if card_ids:
                all_times = []
                for card_id in card_ids:
                    if self.transactions[card_id]:
                        all_times.extend([t["datetime"] for t in self.transactions[card_id]])

                if all_times:
                    min_time = min(all_times)
                    max_time = max(all_times)
                    time_delta = (max_time - min_time) * 0.05
                    self.ax_value.set_xlim(min_time - time_delta, max_time + time_delta)
                    self.ax_limit.set_xlim(min_time - time_delta, max_time + time_delta)
                    self.ax_anomaly.set_xlim(min_time - time_delta, max_time + time_delta)

                    self.ax_anomaly.set_ylim(-0.1, 1.2)

            # Format date on x-axis - include day information
            date_format = '%Y-%m-%d %H:%M:%S' if self.spans_multiple_days(all_times) else '%H:%M:%S'
            self.ax_value.xaxis.set_major_formatter(mdates.DateFormatter(date_format))
            self.ax_limit.xaxis.set_major_formatter(mdates.DateFormatter(date_format))
            self.ax_anomaly.xaxis.set_major_formatter(mdates.DateFormatter(date_format))

            # Rotate date labels for better readability if showing full dates
            if self.spans_multiple_days(all_times):
                for ax in [self.ax_value, self.ax_limit, self.ax_anomaly]:
                    plt.setp(ax.xaxis.get_majorticklabels(), rotation=25, ha='right')

            # Adjust layout
            plt.tight_layout()
        return []

    def spans_multiple_days(self, datetimes):
        """Check if the transactions span multiple days"""
        if not datetimes:
            return False

        # Get unique days
        days = set(dt.date() for dt in datetimes)
        return len(days) > 1
        return []

    def update_map_periodically(self):
        """Periodically update the folium map with transaction locations"""
        while self.running:
            with self.data_lock:
                locations = []
                for card_transactions in self.transactions.values():
                    for transaction in card_transactions:
                        locations.append([transaction["latitude"], transaction["longitude"]])

            if locations:
                self.update_folium_map(locations)

            time.sleep(60)  # Update map every minute

    def update_folium_map(self, locations: List[List[float]]):
        """Update the folium map with new locations"""
        if not hasattr(self, 'map'):
            self.map = folium.Map(location=[np.mean([loc[0] for loc in locations]), np.mean([loc[1] for loc in locations])], zoom_start=13)

        HeatMap(locations).add_to(self.map)

        # Save the map to an HTML file
        map_file = "transactions_map.html"
        self.map.save(map_file)

    def run(self):
        """Run the visualizer"""
        try:
            plt.show()
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self):
        """Shutdown the visualizer"""
        logger.info("Shutting down visualizer...")
        self.running = False
        self.consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Visualize credit card transactions')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--card', type=int, help='Card ID to visualize')
    group.add_argument('--user', type=int, help='User ID to visualize (all cards)')
    args = parser.parse_args()

    try:
        # Create and run the visualizer
        if args.card:
            visualizer = TransactionVisualizer(card_id=args.card)
            logger.info(f"Starting visualization for card ID: {args.card}")
        else:
            visualizer = TransactionVisualizer(user_id=args.user)
            logger.info(f"Starting visualization for user ID: {args.user}")

        visualizer.run()
    except KeyboardInterrupt:
        logger.info("Visualization interrupted by user")
    except Exception as e:
        logger.error(f"Error during visualization: {e}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("Visualization ended")
