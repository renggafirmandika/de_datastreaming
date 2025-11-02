import time
import paho.mqtt.client as mqtt
import json
import sys
import pandas as pd


# MQTT Config
# broker.hivemq.com is a public MQTT broker. 
# If there is a problem with this broker, you can use localhost after installing an MQTT broker (such as Mosquitto) on your local machine.
BROKER = "broker.hivemq.com"
#BROKER = "localhost"
PORT = 1883
TOPIC = "COMP5339/T07G04/facilities"

# define what happens upon connection to the server
def on_connect(client, userdata, connect_flags, reason_code, properties):
    print("Connected with result code " + str(reason_code))
    print(f"Ready to publish to topic: {TOPIC}")

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

# load data for a specific day
def load_day_data(df:pd.DataFrame, day_number:int):
    unique_dates = sorted(df['timestamp'].dt.date.unique())

    if day_number >= len(unique_dates):
        return pd.DataFrame()

    target_date = unique_dates[day_number]
    day_df = df[df['timestamp'].dt.date == target_date].copy()

    return day_df.sort_values(by='timestamp')

# publish data stream 
def publish_data_stream(df:pd.DataFrame):
    # Wait a moment for connection to be fully established
    time.sleep(1)

    unique_dates = sorted(df['timestamp'].dt.date.unique())
    num_days = len(unique_dates)

    iteration = 0

    while True:
        iteration += 1

        for day_number in range(num_days):
            day_data = load_day_data(df, day_number)

            if day_data.empty:
                continue

            current_date = day_data['timestamp'].iloc[0].date()
            print(f"\n{'='*60}")
            print(f"Publishing {current_date} data...")
            print(f"\n{'='*60}")

            for _, r in day_data.iterrows():
                payload = {
                    "timestamp": r["timestamp"].isoformat() if pd.notna(r["timestamp"]) else None,                      # ensure str/ISO if datetime
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
        
        print(f"Data for {current_date} published successfully")

        if day_number < num_days - 1:
            print(f"Waiting 60 seconds before next day's data...")
            time.sleep(60)

# function to stop the MQTT client
def stop_mtqq_client():
    client.loop_stop()
    client.disconnect()

# full pipeline
def publish_via_mqtt_broker(data:pd.DataFrame):
    df = data

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print("Starting continuous data stream publisher...")

    try:
        initialise_mqtt_client()
        publish_data_stream(df)
    except KeyboardInterrupt:
        print("\n\nStopping publisher...")
        stop_mtqq_client()
        print("Disconnected from MQTT broker")
        





