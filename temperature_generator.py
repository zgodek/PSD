#!/usr/bin/env python3
import json
import random
import time
import numpy as np
from datetime import datetime
from kafka import KafkaProducer

# Konfiguracja producenta Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Parametry generatora
NUM_THERMOMETERS = 5
INTERVAL_SEC = 1

# Parametry rozkładu normalnego dla każdego termometru
# (średnia temperatura, odchylenie standardowe)
THERMOMETER_PARAMS = {
    1: (20.0, 2.0),   # Termometr 1: średnia 20°C, odchylenie 2°C
    2: (22.0, 1.5),   # Termometr 2: średnia 22°C, odchylenie 1.5°C
    3: (18.0, 3.0),   # Termometr 3: średnia 18°C, odchylenie 3°C
    4: (25.0, 2.5),   # Termometr 4: średnia 25°C, odchylenie 2.5°C
    5: (15.0, 1.8)    # Termometr 5: średnia 15°C, odchylenie 1.8°C
}

# Co jakiś czas generujemy anomalię
ANOMALY_PROBABILITY = 0.05  # 5% szans na anomalię w każdym pomiarze


def generate_temperature(thermometer_id):
    """Generuje temperaturę z rozkładu normalnego dla danego termometru"""
    mean, std_dev = THERMOMETER_PARAMS[thermometer_id]
    
    # Sprawdź, czy generujemy anomalię
    if random.random() < ANOMALY_PROBABILITY:
        # Anomalia: znaczne odchylenie (3-5 odchyleń standardowych)
        deviation_factor = random.uniform(3.0, 5.0)
        # Losowo wybierz kierunek odchylenia (dodatni lub ujemny)
        direction = 1 if random.random() > 0.5 else -1
        temperature = mean + (direction * deviation_factor * std_dev)
        print(f"ANOMALIA dla termometru {thermometer_id}: {temperature:.2f}°C")
    else:
        # Normalny pomiar z rozkładu normalnego
        temperature = np.random.normal(mean, std_dev)
    
    return round(temperature, 2)


def main():
    print("Starting temperature data generator with normal distribution...")
    try:
        while True:
            current_time = datetime.now().isoformat()
            
            for thermometer_id in range(1, NUM_THERMOMETERS + 1):
                temperature = generate_temperature(thermometer_id)
                
                # Przygotuj dane
                data = {
                    "id_termometru": thermometer_id,
                    "czas_pomiaru": current_time,
                    "temperatura": temperature
                }
                
                # Wyślij dane do Kafki
                producer.send('Temperatura', data)
                print(f"Sent: Termometr {thermometer_id}, Temp: {temperature:.2f}°C")
            
            # Wyślij wszystkie dane
            producer.flush()
            
            # Poczekaj przed wygenerowaniem kolejnych danych
            time.sleep(INTERVAL_SEC)
    
    except KeyboardInterrupt:
        print("Generator stopped by user")
    finally:
        producer.close()
        print("Generator shutdown complete")


if __name__ == "__main__":
    main()
