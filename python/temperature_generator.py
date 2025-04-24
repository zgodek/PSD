#!/usr/bin/env python3
import json
import random
import time
import math
import numpy as np
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

NUM_THERMOMETERS = 5
INTERVAL_SEC = 1

THERMOMETER_PARAMS = {
    1: (20.0, 2.0),   
    2: (22.0, 1.5),   
    3: (18.0, 3.0),  
    4: (25.0, 2.5),  
    5: (15.0, 1.8)   
}

TREND_PERIOD = 300  
TREND_AMPLITUDE = { 
    1: 5.0,  
    2: 4.0, 
    3: 6.0, 
    4: 5.5,  
    5: 4.5  
}

ANOMALY_PROBABILITY = 0.05  

start_time = time.time()


def get_trend_value(thermometer_id, elapsed_time):
    """Oblicza wartość trendu dla danego termometru w danym czasie"""
    phase = (elapsed_time % TREND_PERIOD) / TREND_PERIOD * 2 * math.pi
    trend = math.sin(phase) * TREND_AMPLITUDE[thermometer_id]
    return trend


def generate_temperature(thermometer_id, elapsed_time):
    """Generuje temperaturę z rozkładu normalnego dla danego termometru z uwzględnieniem trendu"""
    base_mean, std_dev = THERMOMETER_PARAMS[thermometer_id]
    
    trend_value = get_trend_value(thermometer_id, elapsed_time)
    current_mean = base_mean + trend_value
    
    if random.random() < ANOMALY_PROBABILITY:
        deviation_factor = random.uniform(3.0, 5.0)
        direction = 1 if random.random() > 0.5 else -1
        temperature = current_mean + (direction * deviation_factor * std_dev)
    else:
        temperature = np.random.normal(current_mean, std_dev)
    
    return round(temperature, 2), round(current_mean, 2)


def main():
    print("Starting temperature data generator with trending normal distribution...")
    start_time = time.time()
    
    try:
        while True:
            current_time = datetime.now().isoformat()
            elapsed_time = time.time() - start_time
            
            for thermometer_id in range(1, NUM_THERMOMETERS + 1):
                temperature, _ = generate_temperature(thermometer_id, elapsed_time)
                
                data = {
                    "id_termometru": thermometer_id,
                    "czas_pomiaru": current_time,
                    "temperatura": temperature
                }
                
                producer.send('Temperatura', data)            
            producer.flush()
            
            time.sleep(INTERVAL_SEC)
    
    except KeyboardInterrupt:
        print("Generator stopped by user")
    finally:
        producer.close()
        print("Generator shutdown complete")


if __name__ == "__main__":
    main()
