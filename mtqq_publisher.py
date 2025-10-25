import time
import paho.mqtt.client as mqtt
import json
import sys
import pandas as pd


# MQTT Config
BROKER = "localhost"   
PORT = 1883
TOPIC = "COMP5339/T07G04/facilities"

df = pd.read_csv("DATA/EXTRACTED/consolidated_facilities_cleaned.csv")

# define what happens upon connection to the server
def on_connect(client, userdata, connect_flags, reason_code, properties):
    print("Connected with result code " + str(reason_code))
    print(f"Ready to publish to topic: {TOPIC}")

# # define what happens upon receiving a message from the server
# def on_message(client, userdata, msg):
#     print(f"Received message on topic {msg.topic}: {msg.payload}")

# setup client
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
def initialise_mqtt_client():
    client.on_connect = on_connect
    try:
        client.connect(BROKER, PORT, 60) 
        client.loop_start()
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}")
        sys.exit(1)

def publish_data_stream(df:pd.DataFrame):
    # Wait a moment for connection to be fully established
    time.sleep(1)
    
    df_to_publish = df.sort_values(by='timestamp').copy()
    print(f"Starting to publish {len(df_to_publish)} records...")

    for _, r in df_to_publish.iterrows():
        payload = {
            "timestamp": r["timestamp"],                      # ensure str/ISO if datetime
            "facility_code": r["facility_code"],
            "facility_name": r.get("facility_name"),
            "lat": float(r["lat"]) if not pd.isna(r.get("lat")) else None,
            "lng": float(r["lng"]) if not pd.isna(r.get("lng")) else None,
            "power": None if pd.isna(r.get("power")) else float(r["power"]),
            "emissions": None if pd.isna(r.get("emissions")) else float(r["emissions"]),
        }
        
        message = json.dumps(payload)
        client.publish(TOPIC, message)
        print(f"[PUBLISHED] {r['facility_code']} @ {r['timestamp']}")

        time.sleep(0.1)

def stop_mtqq_client():
    client.loop_stop()
    client.disconnect()


initialise_mqtt_client()
publish_data_stream(df)
stop_mtqq_client()

