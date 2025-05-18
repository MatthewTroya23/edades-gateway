import paho.mqtt.client as mqtt
import json
import time
import os

# Variable para almacenar el último mensaje recibido
last_message = None
file_path = ""
file_exists = os.path.exists(file_path)

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
        
        # Calcular promedio cada hora
        if time.time() - start_time >= 3600:
            avg_current = round(sum(data_accumulator["avg_current"]) / len(data_accumulator["avg_current"]), 3)
            avg_voltage = round(sum(data_accumulator["avg_voltage"]) / len(data_accumulator["avg_voltage"]), 3)
            total_act_power = round(sum(data_accumulator["act_power"]), 3)
            total_aprt_power = round(sum(data_accumulator["aprt_power"]), 3)
            energy_consumption = total_act_power/len(data_accumulator["act_power"])
            mac = data.get("mac", "")
            uptime = data.get("uptime", 0)
            current = em_status.get("current", 0)
            max_current = round(max(data_accumulator["avg_current"]), 3)
            voltage = em_status.get("voltage", 0)
            max_voltage = round(max(data_accumulator["avg_voltage"]), 3)
            pf = em_status.get("pf", 0)
            freq = em_status.get("freq", 0)

            #print(f"Promedios últimos 60 minutos: Current={avg_current}, Voltage={avg_voltage}, Active Power={avg_act_power}, Apparent Power={avg_aprt_power}")
            with open(file_path, "a") as file:
                if not file_exists:
                    file.write("timestamp,mac,uptime,current,avg_current,max_current,voltage,avg_voltage,max_voltage,total_act_power,total_aprt_power,pf,freq,energy\n")  # Escribir encabezados
                file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')},{mac},{uptime},{current},{avg_current},{max_current},{voltage},{avg_voltage},{max_voltage},{total_act_power},{total_aprt_power},{pf},{freq},{energy_consumption}\n")

            # Reiniciar acumulador y tiempo
            data_accumulator = { "avg_current": []
                                , "avg_voltage": []
                                , "act_power": []
                                , "aprt_power": []
                                }
            start_time = time.time()
    except json.JSONDecodeError:
        print("Error al decodificar JSON")

# Callback cuando se recibe un mensaje
def on_message(client, userdata, msg):
    global last_message
    last_message = msg.payload.decode("utf-8")
    print(f"Mensaje recibido: {last_message}")
    read_data(last_message)

# Configurar cliente MQTT
broker_address = ""  # Cambia esto si el broker está en otra dirección
port =  # Puerto por defecto de MQTT
topic = ""

client = mqtt.Client()
client.on_message = on_message

client.connect(broker_address, port, 60)
client.subscribe(topic)

print(f"Suscrito al tópico {topic}")

# Mantener el cliente en ejecución
client.loop_forever()