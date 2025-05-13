import paho.mqtt.client as mqtt
import time

# Configuración del broker B1 (donde nos suscribimos)
BROKER_B1 = ""  # IP o hostname de rpi
PORT_B1 = 
TOPIC_SUBSCRIBE = ""    

# Configuración del broker B2 (donde publicamos)
BROKER_B2 = "" # IP o hostname de Servidor
PORT_B2 = 
TOPIC_PUBLISH = ""

# Cliente para el broker B2
client_b2 = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    """Callback cuando se conecta al broker B1."""
    if rc == 0:
        print(f"Conectado a {BROKER_B1}, suscrito a {TOPIC_SUBSCRIBE}")
        client.subscribe(TOPIC_SUBSCRIBE)
    else:
        print(f"Error de conexión: {rc}")

def on_message(client, userdata, msg):
    """Callback cuando se recibe un mensaje en B1, se publica en B2."""
    message = msg.payload.decode()
    #print(f"Mensaje recibido: {message}")

    # Publicar en el segundo broker (B2)
    client_b2.connect(BROKER_B2, PORT_B2, 60)
    client_b2.publish(TOPIC_PUBLISH, message)
    client_b2.disconnect()

# Cliente para el broker B1
client_b1 = mqtt.Client()
client_b1.on_connect = on_connect
client_b1.on_message = on_message

# Conectar al broker B1 y comenzar el loop
client_b1.connect(BROKER_B1, PORT_B1, 60)
client_b1.loop_forever()
