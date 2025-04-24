#!/usr/bin/env python3
import os
import logging
import json
import numpy as np
from datetime import datetime
from collections import deque

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common import Types

# Konfiguracja loggera
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Parametry wykrywania anomalii
Z_SCORE_THRESHOLD = 2.5  # Próg Z-score dla wykrywania anomalii
WINDOW_SIZE = 100  # Liczba pomiarów do obliczania statystyk

# Słownik do przechowywania danych dla każdego termometru
thermometer_data = {}

def compute_stats(temperatures, window_size=WINDOW_SIZE):
    """Zwraca średnią i odchylenie standardowe z ostatnich 'window_size' temperatur."""
    if len(temperatures) < 2:  # Potrzebujemy co najmniej 2 pomiarów do obliczenia odchylenia
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
        # Parsuj JSON
        data = json.loads(record)
        
        thermometer_id = data['id_termometru']
        temperature = data['temperatura']
        timestamp = data['czas_pomiaru']
        
        # Inicjalizuj dane dla nowego termometru
        if thermometer_id not in thermometer_data:
            thermometer_data[thermometer_id] = []
        
        # Dodaj nowy pomiar
        thermometer_data[thermometer_id].append(temperature)
        
        # Oblicz statystyki
        mean, std_dev = compute_stats(thermometer_data[thermometer_id])
        
        # Wykryj anomalię
        is_anomaly, z_score = detect_anomaly(temperature, mean, std_dev)
        
        # Jeśli wykryto anomalię, wyślij alarm
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
            return json.dumps(alarm_data)
        
        return None
    
    except Exception as e:
        logger.error(f"Błąd przetwarzania rekordu: {e}")
        return None


def main():
    # Środowisko Flink
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Dodaj JAR Kafka connector do classpath
    kafka_jar = "file:///opt/flink/lib/flink-sql-connector-kafka-1.17.2.jar"
    env.add_jars(kafka_jar)
    
    env.set_parallelism(1)
    
    # Konfiguracja Kafka
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',  # Adres wewnątrz sieci Docker
        'group.id': 'flink-temperature-processor'
    }
    
    # Konsument Kafka (wejście: surowe JSON-stringi z temperaturą)
    kafka_source = FlinkKafkaConsumer(
        topics='Temperatura',  # Poprawiona nazwa tematu
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    kafka_source.set_start_from_latest()
    
    # Dodaj źródło do środowiska
    stream = env.add_source(kafka_source)
    
    # Przetwarzanie danych
    processed = stream.map(
        process_record,
        output_type=Types.STRING()
    ).filter(lambda x: x is not None)
    
    # Producent Kafka (wyjście: JSON z alarmem)
    kafka_producer_props = {
        'bootstrap.servers': 'kafka:9092',  # Adres wewnątrz sieci Docker
        'transaction.timeout.ms': '5000'
    }
    
    kafka_producer = FlinkKafkaProducer(
        topic='Alarm',
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_producer_props
    )
    
    # Dodaj ujście do strumienia
    processed.add_sink(kafka_producer)
    
    # Uruchom zadanie Flink
    logger.info("Starting Flink temperature anomaly detection job...")
    env.execute("Temperature Anomaly Detection")

if __name__ == "__main__":
    main()