import json
import pandas as pd
import folium
from folium.plugins import MarkerCluster
import paho.mqtt.client as mqtt
import streamlit as st
from datetime import datetime
from streamlit_folium import st_folium
import tempfile
import os
import sys
import time
from streamlit_autorefresh import st_autorefresh

# MQTT Config
BROKER = "localhost"   
PORT = 1883
TOPIC = "COMP5339/T07G04/facilities"

# session states
if "records" not in st.session_state:
    st.session_state.records = {}      
if "center" not in st.session_state:
    st.session_state.center = [-25.5, 134.5]  # Australia centroid
if "zoom" not in st.session_state:
    st.session_state.zoom = 4
if "mqtt_started" not in st.session_state:
    st.session_state.mqtt_started = False

# streamlit setup
st.set_page_config(page_title="Electricity Facility Dashboard", layout="wide")
st.title("Real-time Electricity Facility Dashboard")
st.caption("Live feed of power and emissions per facility")

view_mode = st.sidebar.radio("Select view mode:", ["Power (MW)", "Emissions (t CO₂)"])
st_autorefresh(interval=10000)
st.sidebar.write("Data updates automatically as MQTT messages arrive.")

data = st.session_state.records


m = folium.Map(location=st.session_state.center, zoom_start=st.session_state.zoom)
fg = folium.FeatureGroup(name="Facilities")
# mc = MarkerCluster().add_to(fg)

for facility_code, data in data.items():
    val = data['power'] if view_mode == "Power (MW)" else data['emissions']
    popup = (f"<b>{data['facility_name']}</b><br>"
            f"<b>Code:</b> {data['facility_code']}<br>"
            f"<b>Power:</b> {data['power']} MW<br>"
            f"<b>Emissions:</b> {data['emissions']} t<br>"
            f"<b>Updated:</b> {data['timestamp']}")

    if val > 0:
        radius = min(max(val / 10, 5), 30)  # Scale radius based on value
    else:
        radius = 5

    fg.add_child(
        folium.CircleMarker(
            location=[float(data['lat']), float(data['lng'])],
            popup=popup,
            radius=radius,
            tooltip=f"{data['facility_name']} ({val})",
            color='#FF8C00',
            fill=True,
            fillOpacity=0.8
        )
    )
    

m.add_child(fg)

out = st_folium(
    m, key="map", feature_group_to_add=fg,
    center=st.session_state.center, zoom=st.session_state.zoom,
    height=600, width=1000
)

if out and "center" in out and out["center"]:
    st.session_state.center = [out["center"]["lat"], out["center"]["lng"]]
if out and "zoom" in out and out["zoom"]:
    st.session_state.zoom = out["zoom"]

    

# define what happens upon connection to the server
def on_connect(client, userdata, connect_flags, reason_code, properties):
    st.sidebar.success("Connected with result code " + str(reason_code))
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    
    payload = json.loads(msg.payload.decode())

    facility_code = payload.get("facility_code")
    facility_name = payload.get("facility_name")
    lat = payload.get("lat")
    lng = payload.get("lng")
    power = payload.get("power")
    emissions = payload.get("emissions")
    timestamp = payload.get("timestamp")

    st.session_state.records[facility_code] = {
        "facility_code": facility_code,
        "facility_name": facility_name,
        "lat": lat,
        "lng": lng,
        "power": power,
        "emissions": emissions,
        "timestamp": timestamp
    }

# setup client
if not st.session_state.mqtt_started:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(BROKER, PORT, 60) 
        client.loop_forever()
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}")
        sys.exit(1)
    st.session_state.mqtt_started = True

