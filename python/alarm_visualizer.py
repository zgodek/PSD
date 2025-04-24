#!/usr/bin/env python3
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
from datetime import datetime
import threading
import collections

Z_SCORE_THRESHOLD = 2.5

alarm_consumer = KafkaConsumer(
    'Alarm',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='temperature_visualization_alarm'
)

stats_consumer = KafkaConsumer(
    'Statystyki',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='temperature_visualization_stats'
)

# Dane do wizualizacji
alarm_data = collections.defaultdict(list)
stats_data = collections.defaultdict(list) 
max_points = 1200  
data_lock = threading.Lock()


def consume_alarm_data():
    for message in alarm_consumer:
        data = message.value
        thermometer_id = data['id_termometru']
        temperature = data['temperatura']
        mean_temp = data.get('srednia_temperatura', 0)
        z_score = data.get('z_score', 0)
        
        try:
            timestamp = datetime.fromisoformat(data['czas_pomiaru'].replace('Z', '+00:00'))
        except Exception:
            timestamp = datetime.now()  
        
        with data_lock:
            alarm_data[thermometer_id].append((timestamp, temperature, mean_temp, z_score))
            
            if len(alarm_data[thermometer_id]) > max_points:
                alarm_data[thermometer_id] = alarm_data[thermometer_id][-max_points:]
        
        print(f"Alarm: Termometr {thermometer_id}, Czas: {timestamp}, Temperatura: {temperature:.2f}°C, "
              f"Średnia: {mean_temp:.2f}°C, Z-score: {z_score:.2f}")


def consume_stats_data():
    for message in stats_consumer:
        data = message.value
        thermometer_id = data['id_termometru']
        temperature = data['temperatura']
        mean_temp = data.get('srednia_temperatura', 0)
        z_score = data.get('z_score', 0)
        
        try:
            timestamp = datetime.fromisoformat(data['czas_pomiaru'].replace('Z', '+00:00'))
        except Exception:
            timestamp = datetime.now()  
        
        with data_lock:
            stats_data[thermometer_id].append((timestamp, temperature, mean_temp, z_score))
            
            if len(stats_data[thermometer_id]) > max_points:
                stats_data[thermometer_id] = stats_data[thermometer_id][-max_points:]


fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15))
colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']  


def update_plot(frame):
    with data_lock:
        ax1.clear()
        ax2.clear()
        ax3.clear()
        
        all_times = []
        for data_points in stats_data.values():
            if data_points:
                times, _, _, _ = zip(*data_points)
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
        
        all_thermometer_ids = set(list(stats_data.keys()) + list(alarm_data.keys()))
        color_map = {therm_id: colors[i % len(colors)] for i, therm_id in enumerate(sorted(all_thermometer_ids))}
        
        for thermometer_id, color in color_map.items():
            if thermometer_id in stats_data and stats_data[thermometer_id]:
                data_points = stats_data[thermometer_id]
                times, temps, means, z_scores = zip(*data_points)
                
                ax1.plot(times, temps, 'o-', color=color, markersize=3, linewidth=1, label=f'Termometr {thermometer_id}')
                ax1.plot(times, means, '--', color=color, alpha=0.7, linewidth=2, label=f'Średnia {thermometer_id}')
        
        ax1.set_title('Aktualne temperatury ze średnią ruchomą')
        ax1.set_xlabel('Czas')
        ax1.set_ylabel('Temperatura (°C)')
        ax1.grid(True)
        ax1.legend()
        if time_range:
            ax1.set_xlim(time_range)
        
        for thermometer_id, color in color_map.items():
            if thermometer_id in alarm_data and alarm_data[thermometer_id]:
                data_points = alarm_data[thermometer_id]
                times, temps, means, z_scores = zip(*data_points)
                ax2.plot(times, temps, 'o', color=color, markersize=4, label=f'Alarm {thermometer_id}')
                ax3.plot(times, z_scores, 'x', color=color, markersize=4, linewidth=1, label=f'Z-score {thermometer_id}')

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


alarm_thread = threading.Thread(target=consume_alarm_data, daemon=True)
alarm_thread.start()

stats_thread = threading.Thread(target=consume_stats_data, daemon=True)
stats_thread.start()

ani = animation.FuncAnimation(fig, update_plot, interval=1000)

plt.tight_layout()

try:
    plt.show()
except KeyboardInterrupt:
    pass
finally:
    alarm_consumer.close()
    stats_consumer.close()
    print("Visualizer shutdown complete")
