import json
import datetime
import logging
import math
import redis
from collections import deque
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import FlatMapFunction, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Time
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner


WINDOW_SIZE = 10  # in seconds
WINDOW_STEP = 2
REDIS_HOST = "redis"
REDIS_PORT = 6379

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
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


class ParseTransactionList(FlatMapFunction):
    def flat_map(self, msg):
        try:
            data = json.loads(msg)
            if isinstance(data, dict):
                data_list = [data]
            else:
                data_list = data

            for item in data_list:
                yield (item["timestamp"], item["value"], item["card_id"], item["user_id"], item["available_limit"], item["latitude"], item["longitude"])
        except Exception as e:
            logger.error(f"Error parsing message: {e}")


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, previous_element_timestamp):
        return element[0]


class TenTimesTheAverage(ProcessWindowFunction): #zczytaj 50 wartości z bazy na początku
    def open(self, runtime_context):
        self.card_state = runtime_context.get_state(
            ValueStateDescriptor("card_state", Types.PICKLED_BYTE_ARRAY())
        )

    def process(self, key, context, elements):
        results = []
        alarm_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        state = self.card_state.value()
        if state is None:
            state = {}

        for e in elements:
            value = e[1]
            card_id = e[2]

            if card_id not in state:
                state[card_id] = deque(maxlen=50)

            value_history = state[card_id]
            last_avg = sum(value_history) / len(value_history) if value_history else 0

            if len(value_history) >= 10 and value > 10 * last_avg:
                alarm = {
                    "alarm_time": alarm_time,
                    "card_id": card_id,
                    "value": value,
                    "last_avg": last_avg,
                    "alarm_type": "TenTimesTheAverage"
                }
                results.append(json.dumps(alarm))

            value_history.append(value)
            state[card_id] = value_history

        self.card_state.update(state)
        return results


class BurstAfterInactivity(KeyedProcessFunction):
    def open(self, runtime_context):
        self.timestamps_state = runtime_context.get_list_state(
            ListStateDescriptor("recent_timestamps", Types.PICKLED_BYTE_ARRAY())
        )
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def process_element(self, value, context):
        tx_time = value[0]
        card_id = value[2]

        recent_timestamps = list(self.timestamps_state.get()) or []
        if not recent_timestamps:
            redis_key = f"card:{card_id}"
            last_transaction_time = self.redis_client.hget(redis_key, "last_transaction_time")
            try:
                recent_timestamps = [float(last_transaction_time)]
            except Exception as e:
                logger.error(f"Error parsing Redis timestamps for card {card_id}: {e}")
                recent_timestamps = []

        recent_timestamps.append(tx_time)
        recent_timestamps = sorted(recent_timestamps)[-5:]
        self.timestamps_state.update(recent_timestamps)

        long_break_index = None
        for i in range(1, len(recent_timestamps)):
            gap = recent_timestamps[i] - recent_timestamps[i - 1]
            if gap > 30 * 24 * 60 * 60:  # 30 days
                print("Gap bigger than 30 days!!!")
                long_break_index = i
                break

        if long_break_index is not None:
            burst_count = 1
            for i in range(long_break_index + 1, len(recent_timestamps)):
                prev = recent_timestamps[i - 1]
                curr = recent_timestamps[i]
                if curr - prev <= 3600:  # 60 minutes
                    burst_count += 1
                else:
                    break
            if burst_count >= 2:
                alarm = {
                    "alarm_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "card_id": card_id,
                    "burst_start": recent_timestamps[long_break_index],
                    "transactions_in_burst": burst_count,
                    "alarm_type": "DormantCardActivity"
                }
                # recent_timestamps = [recent_timestamps[-1]] #without these the alarms will double if there are 3 transactions in a row
                # self.timestamps_state.update(recent_timestamps)
                return [json.dumps(alarm)]
        return []


class CloseTransactionsNoPin(KeyedProcessFunction):
    def open(self, runtime_context):
        self.last_transaction_state = runtime_context.get_state(
            ValueStateDescriptor("last_transaction", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, value, ctx):
        current_time = value[0]
        current_value = value[1]
        card_id = value[2]

        last_tx = self.last_transaction_state.value()

        if last_tx is not None:
            last_time, last_value = last_tx

            if current_value < 100 and last_value < 100 and (current_time - last_time) < 300:
                alarm = {
                    "alarm_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "card_id": card_id,
                    "previous_transaction_time": last_time,
                    "current_transaction_time": current_time,
                    "previous_value": last_value,
                    "current_value": current_value,
                    "alarm_type": "PinAvoidance"
                }
                self.last_transaction_state.update((current_time, current_value))
                return [json.dumps(alarm)]

        self.last_transaction_state.update((current_time, current_value))
        return []


class RapidTransactions(KeyedProcessFunction):
    def open(self, runtime_context):
        self.last_time_state = runtime_context.get_state(
            ValueStateDescriptor("last_tx_time", Types.DOUBLE())  # zakładamy timestamp w sekundach
        )

    def process_element(self, value, ctx):
        current_time = value[0]
        card_id = value[2]

        last_time = self.last_time_state.value()

        if last_time is not None and (current_time - last_time) < 10:
            alarm = {
                "alarm_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "card_id": card_id,
                "previous_transaction_time": last_time,
                "current_transaction_time": current_time,
                "alarm_type": "RapidTransactions"
            }
            self.last_time_state.update(current_time)
            return [json.dumps(alarm)]

        self.last_time_state.update(current_time)
        return []


class ImpossibleTravel(KeyedProcessFunction):
    def open(self, runtime_context):
        self.last_tx_state = runtime_context.get_state(
            ValueStateDescriptor("last_tx", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element(self, value, ctx):
        timestamp = value[0]
        card_id = value[2]
        lat = value[5]
        lon = value[6]

        last_tx = self.last_tx_state.value()

        if last_tx is not None:
            last_time, last_lat, last_lon = last_tx

            time_diff_sec = timestamp - last_time
            if time_diff_sec <= 0:
                self.last_tx_state.update((timestamp, lat, lon))
                return []

            time_diff_hr = time_diff_sec / 3600.0
            distance_km = calculate_distance(lat, lon, last_lat, last_lon)
            speed = distance_km / time_diff_hr
            if speed >= 900:
                alarm = {
                    "alarm_time": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "card_id": card_id,
                    "current_location": {"lat": lat, "lon": lon},
                    "previous_location": {"lat": last_lat, "lon": last_lon},
                    "distance_km": round(distance_km, 2),
                    "time_diff_sec": time_diff_sec,
                    "estimated_speed_kmh": round(speed, 2),
                    "alarm_type": "ImpossibleTravel"
                }
                self.last_tx_state.update((timestamp, lat, lon))
                return [json.dumps(alarm)]

        self.last_tx_state.update((timestamp, lat, lon))
        return []


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink-trans-consumer'
    }

    consumer = FlinkKafkaConsumer(
        'Transactions',
        SimpleStringSchema(),
        props
    )

    negative_producer = FlinkKafkaProducer(
        topic='NegativeTransaction',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    bigger_10k_producer = FlinkKafkaProducer(
        topic='TransactionBiggerThan10k',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    ten_times_average_producer = FlinkKafkaProducer(
        topic='TransactionTenTimesTheAverage',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    limit_reached_producer = FlinkKafkaProducer(
        topic='LimitReached',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    burst_after_inactivity_producer = FlinkKafkaProducer(
        topic='ManyTransactionsAfterInactivity',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    close_transactions_no_pin_producer = FlinkKafkaProducer(
        topic='ManyTransactionsNoPin',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    rapid_transactions_producer = FlinkKafkaProducer(
        topic='RapidTransactions',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    impossible_travel_producer = FlinkKafkaProducer(
        topic='ImpossibleTravel',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    multi_card_impossible_travel_producer = FlinkKafkaProducer(
        topic='MultiCardImpossibleTravel',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    watermark_strategy = WatermarkStrategy \
        .for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())

    ds = env.add_source(consumer) \
        .flat_map(ParseTransactionList(), output_type=Types.TUPLE([Types.DOUBLE(), Types.DOUBLE(), Types.INT(), Types.INT(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()])) \
        .assign_timestamps_and_watermarks(watermark_strategy)

    negative_value_alarm = ds \
    .filter(lambda x: x[1] < 0) \
    .map(
        lambda x: json.dumps({
            'card_id': x[2],
            'timestamp': x[0],
            'alarm_time': datetime.datetime.now().isoformat(),
            'value': x[1],
            'alarm_type': 'NegativeTransaction'
        }),
        output_type=Types.STRING()
    )

    transaction_above_10k_alarm = ds \
    .filter(lambda x: x[1] >= 10000) \
    .map(
        lambda x: json.dumps({
            'card_id': x[2],
            'timestamp': x[0],
            'alarm_time': datetime.datetime.now().isoformat(),
            'value': x[1],
            'alarm_type': 'VeryHighValue'
        }),
        output_type=Types.STRING()
    )

    ten_times_average_alarm = ds \
        .key_by(lambda x: x[2]) \
        .window(SlidingEventTimeWindows.of(Time.seconds(WINDOW_SIZE), Time.seconds(WINDOW_STEP))) \
        .process(TenTimesTheAverage(), output_type=Types.STRING())
    
    limit_reached_alarm = ds \
    .filter(lambda x: x[4] <= 0.001) \
    .map(
        lambda x: json.dumps({
            'card_id': x[2],
            'timestamp': x[0],
            'alarm_time': datetime.datetime.now().isoformat(),
            'value': x[1],
            'card_limit': x[4],
            'alarm_type': 'LimitExceeded'
        }),
        output_type=Types.STRING()
    )

    burst_after_inactivity_alarm = ds \
        .key_by(lambda x: x[2]) \
        .process(BurstAfterInactivity(), output_type=Types.STRING())
    
    close_transactions_no_pin_alarm = ds \
        .key_by(lambda x: x[2]) \
        .process(CloseTransactionsNoPin(), output_type=Types.STRING())

    rapid_transactions_alarm = ds \
        .key_by(lambda x: x[2]) \
        .process(RapidTransactions(), output_type=Types.STRING())

    impossible_travel_alarm = ds \
        .key_by(lambda x: x[2]) \
        .process(ImpossibleTravel(), output_type=Types.STRING())

    negative_value_alarm.add_sink(negative_producer)
    transaction_above_10k_alarm.add_sink(bigger_10k_producer)
    ten_times_average_alarm.add_sink(ten_times_average_producer)
    limit_reached_alarm.add_sink(limit_reached_producer)
    burst_after_inactivity_alarm.add_sink(burst_after_inactivity_producer)
    close_transactions_no_pin_alarm.add_sink(close_transactions_no_pin_producer)
    rapid_transactions_alarm.add_sink(rapid_transactions_producer)
    impossible_travel_alarm.add_sink(impossible_travel_producer)
    env.execute("Transaction Anomaly Detection")


if __name__ == "__main__":
    main()
