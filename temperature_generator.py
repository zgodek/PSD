#!/usr/bin/env python3
import json
import random
import time
import math
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

# Parametry trendu temperatury
TREND_PERIOD = 300  # Okres pełnego cyklu trendu w sekundach (5 minut)
TREND_AMPLITUDE = {  # Amplituda trendu dla każdego termometru
    1: 5.0,  # Termometr 1: ±5°C
    2: 4.0,  # Termometr 2: ±4°C
    3: 6.0,  # Termometr 3: ±6°C
    4: 5.5,  # Termometr 4: ±5.5°C
    5: 4.5   # Termometr 5: ±4.5°C
}

# Co jakiś czas generujemy anomalię
ANOMALY_PROBABILITY = 0.05  # 5% szans na anomalię w każdym pomiarze

# Licznik czasu dla trendu
start_time = time.time()

def get_trend_value(thermometer_id, elapsed_time):
    """Oblicza wartość trendu dla danego termometru w danym czasie"""
    # Oblicz fazę sinusoidy (0-2π) na podstawie czasu
    phase = (elapsed_time % TREND_PERIOD) / TREND_PERIOD * 2 * math.pi
    # Oblicz wartość sinusoidy (-1 do 1) i przeskaluj przez amplitudę
    trend = math.sin(phase) * TREND_AMPLITUDE[thermometer_id]
    return trend

def generate_temperature(thermometer_id, elapsed_time):
    """Generuje temperaturę z rozkładu normalnego dla danego termometru z uwzględnieniem trendu"""
    base_mean, std_dev = THERMOMETER_PARAMS[thermometer_id]
    
    # Dodaj trend do średniej temperatury
    trend_value = get_trend_value(thermometer_id, elapsed_time)
    current_mean = base_mean + trend_value
    
    # Sprawdź, czy generujemy anomalię
    if random.random() < ANOMALY_PROBABILITY:
        # Anomalia: znaczne odchylenie (3-5 odchyleń standardowych)
        deviation_factor = random.uniform(3.0, 5.0)
        # Losowo wybierz kierunek odchylenia (dodatni lub ujemny)
        direction = 1 if random.random() > 0.5 else -1
        temperature = current_mean + (direction * deviation_factor * std_dev)
        print(f"ANOMALIA dla termometru {thermometer_id}: {temperature:.2f}°C (średnia: {current_mean:.2f}°C)")
    else:
        # Normalny pomiar z rozkładu normalnego
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
                temperature, current_mean = generate_temperature(thermometer_id, elapsed_time)
                
                # Przygotuj dane
                data = {
                    "id_termometru": thermometer_id,
                    "czas_pomiaru": current_time,
                    "temperatura": temperature,
                    "srednia_bazowa": current_mean  # Dodajemy informację o aktualnej średniej bazowej (z trendem)
                }
                
                # Wyślij dane do Kafki
                producer.send('Temperatura', data)
                print(f"Sent: Termometr {thermometer_id}, Temp: {temperature:.2f}°C, Trend Mean: {current_mean:.2f}°C")
            
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
