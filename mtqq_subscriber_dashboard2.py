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
import threading
import queue
from streamlit_autorefresh import st_autorefresh

# MQTT Config
BROKER = "localhost"   
PORT = 1883
TOPIC = "COMP5339/T07G04/facilities"

# streamlit setup
st.set_page_config(page_title="Electricity Facility Dashboard", layout="wide")

# session states
if "records" not in st.session_state:
    st.session_state.records = {}      
if "center" not in st.session_state:
    st.session_state.center = [-25.5, 134.5]  # Australia centroid
if "zoom" not in st.session_state:
    st.session_state.zoom = 4
if "mqtt_started" not in st.session_state:
    st.session_state.mqtt_started = False
if "mqtt_connected" not in st.session_state:
    st.session_state.mqtt_connected = False
if "mqtt_client" not in st.session_state:
    st.session_state.mqtt_client = None
if 'view_mode' not in st.session_state:
    st.session_state.view_mode = 'Power (MW)'
if 'mqtt_thread' not in st.session_state:
    st.session_state.mqtt_thread = None
if 'mqtt_error' not in st.session_state:
    st.session_state.mqtt_error = None
if 'message_queue' not in st.session_state:
    st.session_state.message_queue = queue.Queue()
if 'last_map_update' not in st.session_state:
    st.session_state.last_map_update = 0
if 'map_needs_update' not in st.session_state:
    st.session_state.map_needs_update = True
if 'last_record_count' not in st.session_state:
    st.session_state.last_record_count = 0
if 'last_view_mode' not in st.session_state:
    st.session_state.last_view_mode = 'Power (MW)'

# Connection callbacks
def on_connect(client, userdata, connect_flags, reason_code, properties):
    try:
        message_queue = userdata
        if reason_code == 0:
            message_queue.put({"type": "connection", "connected": True})
            client.subscribe(TOPIC)
        else:
            message_queue.put({"type": "connection", "connected": False})
    except Exception as e:
        print(f"Error in on_connect: {e}")

def on_message(client, userdata, msg):
    try:
        message_queue = userdata
        payload = json.loads(msg.payload.decode())

        facility_code = payload.get("facility_code")
        facility_name = payload.get("facility_name")
        lat = payload.get("lat")
        lng = payload.get("lng")
        power = payload.get("power")
        emissions = payload.get("emissions")
        timestamp = payload.get("timestamp")

        message_queue.put({
            "type": "facility_data",
            "facility_code": facility_code,
            "data": {
                "facility_code": facility_code,
                "facility_name": facility_name,
                "lat": lat,
                "lng": lng,
                "power": power,
                "emissions": emissions,
                "timestamp": timestamp
            }
        })
    except Exception as e:
        print(f"Error processing MQTT message: {e}")

# MQTT thread function
def mqtt_loop_thread(message_queue):
    """Run MQTT client in a separate daemon thread"""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, userdata=message_queue)
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(BROKER, PORT, 60)
        client.loop_forever()
    except Exception as e:
        message_queue.put({"type": "error", "message": str(e)})
        message_queue.put({"type": "connection", "connected": False})

# Setup client
def initialise_client():
    if not st.session_state.mqtt_started:
        thread = threading.Thread(
            target=mqtt_loop_thread, 
            args=(st.session_state.message_queue,),
            daemon=True
        )
        thread.start()
        st.session_state.mqtt_thread = thread
        st.session_state.mqtt_started = True

# Process messages from queue
def process_queue():
    """Process all pending messages from the queue"""
    message_queue = st.session_state.message_queue
    processed = 0
    
    while not message_queue.empty():
        try:
            msg = message_queue.get_nowait()
            
            if msg["type"] == "connection":
                st.session_state.mqtt_connected = msg["connected"]
            elif msg["type"] == "facility_data":
                st.session_state.records[msg["facility_code"]] = msg["data"]
                processed += 1
            elif msg["type"] == "error":
                st.session_state.mqtt_error = msg["message"]
                
        except queue.Empty:
            break
        except Exception as e:
            print(f"Error processing queue message: {e}")
    
    return processed

def visualise(data, view_mode="Power (MW)", force_update=False):
    # Check if we need to rebuild the map
    current_count = len(data)
    
    # Always rebuild if forced, or if record count changed, or view mode changed
    data_changed = (current_count != st.session_state.last_record_count)
    view_changed = (view_mode != st.session_state.last_view_mode)
    
    # ALWAYS update the map when called - removed the skip logic
    m = folium.Map(location=st.session_state.center, zoom_start=st.session_state.zoom)
    fg = folium.FeatureGroup(name="Facilities")

    for facility_code, facility_data in data.items():
        val = facility_data['power'] if view_mode == "Power (MW)" else facility_data['emissions']
        popup = (f"<b>{facility_data['facility_name']}</b><br>"
                f"<b>Code:</b> {facility_data['facility_code']}<br>"
                f"<b>Power:</b> {facility_data['power']} MW<br>"
                f"<b>Emissions:</b> {facility_data['emissions']} t<br>"
                f"<b>Updated:</b> {facility_data['timestamp']}")

        if val > 0:
            radius = min(max(val / 10, 5), 30)
        else:
            radius = 5

        fg.add_child(
            folium.CircleMarker(
                location=[float(facility_data['lat']), float(facility_data['lng'])],
                popup=popup,
                radius=radius,
                tooltip=f"{facility_data['facility_name']} ({val})",
                color='#FF8C00',
                fill=True,
                fillOpacity=0.8
            )
        )

    m.add_child(fg)

    # Use a static key so the map state is preserved
    out = st_folium(
        m, 
        key="facility_map",
        feature_group_to_add=fg,
        center=st.session_state.center, 
        zoom=st.session_state.zoom,
        height=600, 
        width=1000
    )

    # Update session state with any user interactions
    if out:
        if out.get("center"):
            st.session_state.center = [out["center"]["lat"], out["center"]["lng"]]
        if out.get("zoom"):
            st.session_state.zoom = out["zoom"]
    
    # Update tracking variables
    st.session_state.last_record_count = current_count
    st.session_state.last_view_mode = view_mode


def main():
    st.title("Real-time Electricity Facility Dashboard")
    st.caption("Live feed of power and emissions per facility")

    # Initialize MQTT client once
    initialise_client()

    # Process any pending MQTT messages
    new_messages = process_queue()

    # Sidebar status
    if st.session_state.mqtt_connected:
        st.sidebar.success("✅ Connected to MQTT")
    else:
        st.sidebar.warning("⏳ Connecting to MQTT...")

    if st.session_state.mqtt_error:
        st.sidebar.error(f"Error: {st.session_state.mqtt_error}")

    # View mode selection
    view_mode = st.sidebar.radio("Select view mode:", ["Power (MW)", "Emissions (t CO₂)"])
    st.session_state.view_mode = view_mode

    # Stats
    num_facilities = len(st.session_state.records)
    st.sidebar.metric("Facilities Tracked", num_facilities)
    if new_messages > 0:
        st.sidebar.info(f"🆕 {new_messages} new updates")

    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Map Now"):
        st.rerun()

    # Show status
    st.info(f"📊 Tracking {len(st.session_state.records)} facilities • Auto-updates every 10 seconds")
    
    # Always draw the map with current data
    if st.session_state.records:
        visualise(st.session_state.records, view_mode)
    else:
        visualise({}, view_mode)
        if len(st.session_state.records) == 0:
            st.warning("Waiting for MQTT messages... No facilities data available yet.")

    # Auto-refresh every 10 seconds (not 2!) to update map with new data
    count = st_autorefresh(interval=10000, limit=None, key="auto_refresh")

main()