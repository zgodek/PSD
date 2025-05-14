import json
import datetime
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import FlatMapFunction, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner


WINDOW_SIZE = 10  # in seconds
WINDOW_STEP = 1


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ParseTransactionList(FlatMapFunction):
    def flat_map(self, msg):
        try:
            data = json.loads(msg)
            if isinstance(data, dict):
                data_list = [data]
            else:
                data_list = data

            for item in data_list:
                #print(f"[ParseTransactionList] {item['card_id']} {item['user_id']} {item['value']}")
                yield (item["timestamp"], item["value"], item["card_id"], item["user_id"])
        except Exception as e:
            logger.error(f"Error parsing message: {e}")


class TransactionBiggerThanAvg(ProcessWindowFunction):
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
                state[card_id] = {"sum": 0.0, "count": 0}

            card_data = state[card_id]
            last_avg = card_data["sum"] / card_data["count"] if card_data["count"] > 0 else 0
            if last_avg > 0 and value > 10*last_avg:
                alarm = {
                    "alarm_time": alarm_time,
                    "card_id": card_id,
                    "value": value,
                    "last_avg": last_avg,
                }
                results.append(json.dumps(alarm))

            card_data["sum"] += value
            card_data["count"] += 1
            state[card_id] = card_data

        self.card_state.update(state)
        return results


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, previous_element_timestamp):
        return element[0]


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
        topic='NegativeTransactionAlarm',
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

    watermark_strategy = WatermarkStrategy \
        .for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())

    ds = env.add_source(consumer) \
        .flat_map(ParseTransactionList(), output_type=Types.TUPLE([Types.DOUBLE(), Types.DOUBLE(), Types.INT(), Types.INT()])) \
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
            'alarm_type': 'TransactionBiggerThan10k'
        }),
        output_type=Types.STRING()
    )

    ten_times_average_alarm = ds \
        .key_by(lambda x: x[2]) \
        .window(SlidingEventTimeWindows.of(Time.seconds(WINDOW_SIZE), Time.seconds(WINDOW_STEP))) \
        .process(TransactionBiggerThanAvg(), output_type=Types.STRING())

    negative_value_alarm.print()
    transaction_above_10k_alarm.print()
    ten_times_average_alarm.print()
    negative_value_alarm.add_sink(negative_producer)
    transaction_above_10k_alarm.add_sink(bigger_10k_producer)
    ten_times_average_alarm.add_sink(ten_times_average_producer)

    env.execute("Transaction Anomaly Detection")


if __name__ == "__main__":
    main()
