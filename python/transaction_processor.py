import json
import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import FlatMapFunction, MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner

WINDOW_SIZE = 2  # in seconds

class ParseTransactionList(FlatMapFunction):
    def flat_map(self, msg):
        try:
            data = json.loads(msg)
            if isinstance(data, dict):
                data_list = [data]
            else:
                data_list = data

            for item in data_list:
                print(f"[ParseTransactionList] {item['card_id']} {item['user_id']} {item['value']}")
                yield (item["timestamp"], item["value"], item["card_id"])
        except Exception as e:
            print(f"Error parsing message: {e}")


class LogTransaction(MapFunction):
    def map(self, value):
        print(f"[LogTransaction] Raw value: {value}")
        return value


class TransactionBiggerThan10k(ProcessWindowFunction):
    def process(self, key, context, elements):
        transactions_by_card = {}

        for e in elements:
            value = e[1]
            card_id = e[2]
            if card_id not in transactions_by_card:
                transactions_by_card[card_id] = []
            transactions_by_card[card_id].append(value)

        results = []
        alarm_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for card_id, values in transactions_by_card.items():
            avg_value = sum(values) / len(values)
            print(f"[Window] Card {card_id}: Average value = {avg_value}")
            if avg_value > 10000:
                alarm = {
                    "alarm_time": alarm_time,
                    "average_value": avg_value,
                    "card_id": card_id
                }
                results.append(json.dumps(alarm))

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

    producer = FlinkKafkaProducer(
        topic='Alarm',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    watermark_strategy = WatermarkStrategy \
        .for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())

    ds = env.add_source(consumer) \
        .flat_map(ParseTransactionList(), output_type=Types.TUPLE([Types.DOUBLE(), Types.DOUBLE(), Types.INT()])) \
        .assign_timestamps_and_watermarks(watermark_strategy)

    alarms = ds.map(LogTransaction()) \
        .key_by(lambda x: x[2]) \
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE))) \
        .process(TransactionBiggerThan10k(), output_type=Types.STRING())

    alarms.print()
    alarms.add_sink(producer)

    env.execute("Transaction Anomaly Detection")


if __name__ == "__main__":
    main()
