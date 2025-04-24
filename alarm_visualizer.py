#!/usr/bin/env python3
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
from datetime import datetime
import threading
import collections

# Konfiguracja konsumenta Kafka
consumer = KafkaConsumer(
    'Alarm',
    bootstrap_servers=['localhost:29092'],  # Zmiana na port 29092 dla komunikacji z hostem
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='temperature_visualization3'
)

# Dane do wizualizacji
alarm_data = collections.defaultdict(list)  # id_termometru -> [(czas, temperatura, srednia, z_score), ...]
max_points = 100  # Maksymalna liczba punktów do wyświetlenia

# Blokada dla bezpiecznego dostępu do danych z wielu wątków
data_lock = threading.Lock()


# Funkcja do odbierania danych z Kafki w osobnym wątku
def consume_kafka_data():
    for message in consumer:
        data = message.value
        thermometer_id = data['id_termometru']
        temperature = data['temperatura']
        mean_temp = data.get('srednia_temperatura', 0)
        z_score = data.get('z_score', 0)
        
        # Konwersja czasu z formatu ISO do obiektu datetime
        try:
            timestamp = datetime.fromisoformat(data['czas_alarmu'].replace('Z', '+00:00'))
        except:
            timestamp = datetime.now()  # Fallback jeśli format czasu jest nieprawidłowy
        
        with data_lock:
            # Dodaj nowy punkt danych
            alarm_data[thermometer_id].append((timestamp, temperature, mean_temp, z_score))
            
            # Ogranicz liczbę punktów
            if len(alarm_data[thermometer_id]) > max_points:
                alarm_data[thermometer_id] = alarm_data[thermometer_id][-max_points:]
        
        print(f"Alarm: Termometr {thermometer_id}, Czas: {timestamp}, Temperatura: {temperature:.2f}°C, "
              f"Średnia: {mean_temp:.2f}°C, Z-score: {z_score:.2f}")

# Inicjalizacja wykresu
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
lines = {}  # id_termometru -> line object

def update_plot(frame):
    with data_lock:
        # Wyczyść wykresy
        ax1.clear()
        ax2.clear()
        
        # Rysuj dane dla każdego termometru
        for thermometer_id, data_points in alarm_data.items():
            if data_points:
                times, temps, means, z_scores = zip(*data_points)
                
                # Wykres temperatur
                ax1.plot(times, temps, 'o-', label=f'Termometr {thermometer_id}')
                ax1.plot(times, means, '--', alpha=0.5, label=f'Średnia {thermometer_id}')
                ax1.set_title('Alarmy temperaturowe')
                ax1.set_xlabel('Czas')
                ax1.set_ylabel('Temperatura (°C)')
                ax1.grid(True)
                ax1.legend()
                
                # Wykres z-score
                ax2.plot(times, z_scores, 'x-', label=f'Z-score {thermometer_id}')
                ax2.set_title('Z-score temperatur')
                ax2.set_xlabel('Czas')
                ax2.set_ylabel('Z-score')
                ax2.grid(True)
                ax2.legend()
                ax2.axhline(y=2.5, color='r', linestyle='-', alpha=0.3, label='Próg alarmu')
    
    return []

# Uruchom wątek konsumenta Kafka
kafka_thread = threading.Thread(target=consume_kafka_data, daemon=True)
kafka_thread.start()

# Uruchom animację wykresu
ani = animation.FuncAnimation(fig, update_plot, interval=1000)  # Aktualizuj co 1 sekundę

# Wyświetl wykres
plt.tight_layout()

# Zamknij konsumenta po zakończeniu
try:
    plt.show()
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Visualizer shutdown complete")
