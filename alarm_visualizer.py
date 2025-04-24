#!/usr/bin/env python3
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
from datetime import datetime
import threading
import collections

# Próg Z-score dla wykrywania anomalii (taki sam jak w temperature_processor.py)
Z_SCORE_THRESHOLD = 2.5

# Konfiguracja konsumentów Kafka
alarm_consumer = KafkaConsumer(
    'Alarm',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='temperature_visualization_alarm'
)

temperature_consumer = KafkaConsumer(
    'Temperatura',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='temperature_visualization_temp'
)

# Dane do wizualizacji
alarm_data = collections.defaultdict(list)  # id_termometru -> [(czas, temperatura, srednia, z_score), ...]
temperature_data = collections.defaultdict(list)  # id_termometru -> [(czas, temperatura, srednia_bazowa), ...]
max_points = 1000  # Maksymalna liczba punktów do wyświetlenia

# Blokada dla bezpiecznego dostępu do danych z wielu wątków
data_lock = threading.Lock()


# Funkcja do odbierania danych alarmów z Kafki w osobnym wątku
def consume_alarm_data():
    for message in alarm_consumer:
        data = message.value
        thermometer_id = data['id_termometru']
        temperature = data['temperatura']
        mean_temp = data.get('srednia_temperatura', 0)
        z_score = data.get('z_score', 0)
        
        # Konwersja czasu z formatu ISO do obiektu datetime
        try:
            # Używamy czasu pomiaru zamiast czasu alarmu dla spójności z wykresem temperatur
            timestamp = datetime.fromisoformat(data['czas_pomiaru'].replace('Z', '+00:00'))
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


# Funkcja do odbierania danych temperatur z Kafki w osobnym wątku
def consume_temperature_data():
    for message in temperature_consumer:
        data = message.value
        thermometer_id = data['id_termometru']
        temperature = data['temperatura']
        base_mean = data.get('srednia_bazowa', 0)  # Średnia bazowa z trendem
        
        # Konwersja czasu z formatu ISO do obiektu datetime
        try:
            timestamp = datetime.fromisoformat(data['czas_pomiaru'].replace('Z', '+00:00'))
        except:
            timestamp = datetime.now()  # Fallback jeśli format czasu jest nieprawidłowy
        
        with data_lock:
            # Dodaj nowy punkt danych
            temperature_data[thermometer_id].append((timestamp, temperature, base_mean))
            
            # Ogranicz liczbę punktów
            if len(temperature_data[thermometer_id]) > max_points:
                temperature_data[thermometer_id] = temperature_data[thermometer_id][-max_points:]


# Inicjalizacja wykresu
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15))
colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']  # Kolory dla różnych termometrów

def update_plot(frame):
    with data_lock:
        # Wyczyść wykresy
        ax1.clear()
        ax2.clear()
        ax3.clear()
        
        # Ustal wspólny zakres czasu dla wszystkich wykresów
        all_times = []
        for data_points in temperature_data.values():
            if data_points:
                times, _, _ = zip(*data_points)
                all_times.extend(times)
        
        for data_points in alarm_data.values():
            if data_points:
                times, _, _, _ = zip(*data_points)
                all_times.extend(times)
        
        if all_times:
            min_time = min(all_times)
            max_time = max(all_times)
            time_range = (min_time, max_time)
        else:
            time_range = None
        
        # Rysuj dane temperatur dla każdego termometru
        for i, (thermometer_id, data_points) in enumerate(temperature_data.items()):
            if data_points:
                times, temps, base_means = zip(*data_points)
                color = colors[i % len(colors)]
                
                # Wykres temperatur
                ax1.plot(times, temps, 'o', color=color, label=f'Termometr {thermometer_id}')
                ax1.plot(times, base_means, '--', color=color, alpha=0.5, label=f'Trend {thermometer_id}')
        
        ax1.set_title('Aktualne temperatury z trendem')
        ax1.set_xlabel('Czas')
        ax1.set_ylabel('Temperatura (°C)')
        ax1.grid(True)
        ax1.legend()
        if time_range:
            ax1.set_xlim(time_range)
        
        # Rysuj dane alarmów dla każdego termometru
        for i, (thermometer_id, data_points) in enumerate(alarm_data.items()):
            if data_points:
                times, temps, means, z_scores = zip(*data_points)
                color = colors[i % len(colors)]
                
                # Wykres temperatur alarmowych
                ax2.plot(times, temps, 'o', color=color, label=f'Alarm {thermometer_id}')
                
                # Wykres z-score
                ax3.plot(times, z_scores, 'x-', color=color, label=f'Z-score {thermometer_id}')
        
        ax2.set_title('Alarmy temperaturowe')
        ax2.set_xlabel('Czas')
        ax2.set_ylabel('Temperatura (°C)')
        ax2.grid(True)
        ax2.legend()
        if time_range:
            ax2.set_xlim(time_range)
        
        ax3.set_title('Z-score temperatur')
        ax3.set_xlabel('Czas')
        ax3.set_ylabel('Z-score')
        ax3.grid(True)
        ax3.legend()
        ax3.axhline(y=Z_SCORE_THRESHOLD, color='r', linestyle='-', alpha=0.3, label='Próg alarmu')
        if time_range:
            ax3.set_xlim(time_range)
    
    return []

# Uruchom wątki konsumentów Kafka
alarm_thread = threading.Thread(target=consume_alarm_data, daemon=True)
alarm_thread.start()

temperature_thread = threading.Thread(target=consume_temperature_data, daemon=True)
temperature_thread.start()

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
    alarm_consumer.close()
    temperature_consumer.close()
    print("Visualizer shutdown complete")
