import time
import csv
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt
import os
import json

# Variable para almacenar el último mensaje recibido
last_message = None
file_path = ""
file_path_2 = ""
file_exists = os.path.exists(file_path)
file_exists_2 = os.path.exists(file_path_2)

# Variables para acumulación de datos
data_accumulator = { "avg_current": []
                    , "avg_voltage": []
                    , "act_power": []
                    , "aprt_power": []
                    }
start_time = time.time()

# Función para almacenar datos y calcular promedio cada hora
def read_data(message):
    global data_accumulator, start_time
    try:
        data = json.loads(message)
        em_status = data.get("emStatus", {})
        
        # Acumular datos
        data_accumulator["avg_current"].append(em_status.get("current", 0))
        data_accumulator["avg_voltage"].append(em_status.get("voltage", 0))
        data_accumulator["act_power"].append(em_status.get("act_power", 0))
        data_accumulator["aprt_power"].append(em_status.get("aprt_power", 0))

        with open(file_path_2, "a") as file:
            if not file_exists_2:
                file.write("timestamp,avg_current,avg_voltage,total_act_power,total_aprt_power\n")  # Escribir encabezados
            file.write(f'''{time.strftime('%Y-%m-%d %H:%M:%S')},{data_accumulator["avg_current"][-1]},{data_accumulator["avg_voltage"][-1]},{data_accumulator["act_power"][-1]},{data_accumulator["aprt_power"][-1]}\n''')

        
    except json.JSONDecodeError:
        print("Error al decodificar JSON")

# Función para esperar hasta la próxima hora
def wait_until_next_hour():
    """Espera hasta la siguiente hora en punto."""
    now = datetime.now()
    #next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    next_hour = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    sleep_time = (next_hour - now).total_seconds()
    print(f"Esperando {sleep_time} segundos hasta {next_hour}")
    time.sleep(sleep_time)

# Callback cuando se recibe un mensaje
def on_message(client, userdata, msg):
    global last_message
    last_message = msg.payload.decode("utf-8")
    print(f"Mensaje recibido: {last_message}")
    read_data(last_message)

# Configurar cliente MQTT
broker_address = ""  # Cambia esto si el broker está en otra dirección
port =   # Puerto por defecto de MQTT
topic = ""

client = mqtt.Client()
client.on_message = on_message

client.connect(broker_address, port, 60)
client.subscribe(topic)

print(f"Suscrito al tópico {topic}")

# Mantener el cliente en ejecución
client.loop_start()

while True:
    wait_until_next_hour()

    if data_accumulator["avg_current"]:
        data = json.loads(last_message)
        em_status = data.get("emStatus", {})
        avg_current = round(sum(data_accumulator["avg_current"]) / len(data_accumulator["avg_current"]), 3)
        avg_voltage = round(sum(data_accumulator["avg_voltage"]) / len(data_accumulator["avg_voltage"]), 3)
        total_act_power = round(sum(data_accumulator["act_power"]), 3)
        total_aprt_power = round(sum(data_accumulator["aprt_power"]), 3)
        energy_consumption = round((total_act_power / len(data_accumulator["act_power"])) / 60, 2)
        mac = data.get("mac", "")
        uptime = data.get("uptime", 0)
        current = em_status.get("current", 0)
        max_current = round(max(data_accumulator["avg_current"]), 3)
        voltage = em_status.get("voltage", 0)
        max_voltage = round(max(data_accumulator["avg_voltage"]), 3)
        pf = em_status.get("pf", 0)
        freq = em_status.get("freq", 0)

        #print(f"Promedios últimos 60 minutos: Current={avg_current}, Voltage={avg_voltage}, Active Power={total_act_power}, Apparent Power={total_aprt_power}...")
        with open(file_path, "a") as file:
            if not file_exists:
                file.write("timestamp,mac,uptime,energy,current,avg_current,max_current,voltage,avg_voltage,max_voltage,total_act_power,total_aprt_power,pf,freq\n")  # Escribir encabezados
            file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')},{energy_consumption},{mac},{uptime},{current},{avg_current},{max_current},{voltage},{avg_voltage},{max_voltage},{total_act_power},{total_aprt_power},{pf},{freq}\n")

        # Reiniciar acumulador y tiempo
        data_accumulator = { "avg_current": []
                            , "avg_voltage": []
                            , "act_power": []
                            , "aprt_power": []
                            }