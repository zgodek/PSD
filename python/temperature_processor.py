#!/usr/bin/env python3
import logging
import json
import numpy as np
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common import Types

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

Z_SCORE_THRESHOLD = 2.5  
WINDOW_SIZE = 100 

thermometer_data = {}


def compute_stats(temperatures, window_size=WINDOW_SIZE):
    """Zwraca średnią i odchylenie standardowe z ostatnich 'window_size' temperatur."""
    if len(temperatures) < 2:  
        return np.mean(temperatures) if temperatures else 0, 0
    
    if len(temperatures) < window_size:
        return np.mean(temperatures), np.std(temperatures, ddof=1)
    
    return np.mean(temperatures[-window_size:]), np.std(temperatures[-window_size:], ddof=1)


def detect_anomaly(temperature, mean, std_dev):
    """Wykrywa anomalię na podstawie Z-score."""
    if std_dev == 0:
        return False, 0
    
    z_score = abs((temperature - mean) / std_dev)
    return z_score > Z_SCORE_THRESHOLD, z_score


def process_record(record):
    """Przetwarza pojedynczy rekord z Kafki."""
    try:
        data = json.loads(record)
        
        thermometer_id = data['id_termometru']
        temperature = data['temperatura']
        timestamp = data['czas_pomiaru']
        
        if thermometer_id not in thermometer_data:
            thermometer_data[thermometer_id] = []
        
        thermometer_data[thermometer_id].append(temperature)
        
        mean, std_dev = compute_stats(thermometer_data[thermometer_id])
        
        is_anomaly, z_score = detect_anomaly(temperature, mean, std_dev)
        
        stats_data = {
            'id_termometru': thermometer_id,
            'czas_pomiaru': timestamp,
            'temperatura': temperature,
            'srednia_temperatura': round(mean, 2),
            'odchylenie': round(std_dev, 2),
            'z_score': round(z_score, 2)
        }
        
        if is_anomaly:
            alarm_data = {
                'id_termometru': thermometer_id,
                'czas_pomiaru': timestamp,
                'czas_alarmu': datetime.now().isoformat(),
                'temperatura': temperature,
                'srednia_temperatura': round(mean, 2),
                'odchylenie': round(std_dev, 2),
                'z_score': round(z_score, 2)
            }
            
            logger.info(f"ANOMALIA: Termometr {thermometer_id}, Temp: {temperature}°C, Z-score: {z_score:.2f}")
            return (json.dumps(alarm_data), json.dumps(stats_data))
        
        return ("", json.dumps(stats_data))
    
    except Exception as e:
        logger.error(f"Błąd przetwarzania rekordu: {e}")
        return ("", "")  


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    kafka_jar = "file:///opt/flink/lib/flink-sql-connector-kafka-1.17.2.jar"
    env.add_jars(kafka_jar)
    
    env.set_parallelism(1)
    
    kafka_props = {
        'bootstrap.servers': 'kafka:9092', 
        'group.id': 'flink-temperature-processor'
    }
    
    kafka_source = FlinkKafkaConsumer(
        topics='Temperatura', 
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    kafka_source.set_start_from_latest()
    
    stream = env.add_source(kafka_source)
    
    processed = stream.map(
        lambda x: process_record(x),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )
    
    alarm_stream = processed.map(
        lambda x: x[0],
        output_type=Types.STRING()
    ).filter(lambda x: x != "") 
    
    stats_stream = processed.map(
        lambda x: x[1],
        output_type=Types.STRING()
    ).filter(lambda x: x != "") 
    
    kafka_producer_props = {
        'bootstrap.servers': 'kafka:9092',  
        'transaction.timeout.ms': '5000'
    }
    
    alarm_producer = FlinkKafkaProducer(
        topic='Alarm',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_producer_props
    )
    
    stats_producer = FlinkKafkaProducer(
        topic='Statystyki',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_producer_props
    )
    
    alarm_stream.add_sink(alarm_producer)
    stats_stream.add_sink(stats_producer)
    
    logger.info("Starting Flink temperature anomaly detection job...")
    env.execute("Temperature Anomaly Detection")


if __name__ == "__main__":
    main()