import json
import threading
from collections import defaultdict
from datetime import datetime
import queue
import duckdb
import pandas as pd
from datetime import timedelta
import time

import paho.mqtt.client as mqtt
from dash import Dash, html, dcc, Input, Output, State
import plotly.graph_objects as go

# MQTT Config
BROKER = "broker.hivemq.com"
PORT = 1883
FACILITY_TOPIC = "COMP5339/T07G04/facilities"
MARKET_TOPIC = "COMP5339/T07G04/market"

TIME_BUCKET_MINUTES = 5

facility_queue = queue.Queue()
market_queue = queue.Queue()

facility_buckets = defaultdict(lambda: defaultdict(dict))  
market_buckets = defaultdict(lambda: defaultdict(dict))

# Global state storage for latest facility readings
facilities_data = {}
facilities_metadata = {}
mqtt_connected = False

# Fuel type standardization
FUEL_CANONICALS = {
    "battery": "Battery",
    "bioenergy": "Bioenergy",
    "bagasse": "Bagasse",
    "browncoal": "Brown Coal",
    "blackcoal": "Black Coal",
    "coal": "Coal",
    "diesel": "Diesel",
    "distillate": "Diesel",
    "gas": "Gas",
    "wastecoalminegas": "Waste Coal Mine Gas",
    "landfillgas": "Landfill Gas",
    "hydro": "Hydro",
    "wind": "Wind",
    "solar": "Solar",
    "wood": "Wood",
}
def normalize_fuel(val) -> str:
    if val is None:
        return "Other"
    s = str(val).strip()
    key = s.lower().replace(" ", "")
    return FUEL_CANONICALS.get(key, s)

def get_data_from_db():
    con = duckdb.connect("energy_dw.duckdb", read_only=True)

    facilities_metadata_df = con.execute("""
        SELECT 
            facility_code,
            facility_name,
            network_region as region,
            lat,
            lng,
            fuel_type
        FROM facility_opennem_mapping
    """).df()

    metadata = facilities_metadata_df.set_index('facility_code').to_dict('index')

    print(f"✓ Loaded {len(metadata)} facilities from Database")
    
    con.close()

    return metadata

# Marker colors by fuel type
FUEL_COLORS = {
    "Battery": "#A855F7",
    "Bioenergy": "#16A34A",
    "Bagasse": "#84CC16",
    "Brown Coal": "#8B5E34",
    "Black Coal": "#6B7280",
    "Coal": "#4B5563",
    "Diesel": "#EF4444",
    "Gas": "#F59E0B",
    "Waste Coal Mine Gas": "#FB923C",
    "Landfill Gas": "#FBBF24",
    "Hydro": "#3B82F6",
    "Wind": "#22C55E",
    "Solar": "#FCD34D",
    "Wood": "#9CA3AF",
    "Other": "#9CA3AF",
}

def fmt2(x):
    """Format numbers to 2 decimals."""
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "0.00"

def parse_ts(ts):
    """Parse timestamp safely; return very small datetime if invalid."""
    try:
        return datetime.fromisoformat(str(ts))
    except Exception:
        return datetime.min

# MQTT callbacks
def on_connect(client, userdata, flags, reason_code, properties=None):
    """Subscribe to topic once connected."""
    global mqtt_connected
    mqtt_connected = (reason_code == 0)
    if mqtt_connected:
        client.subscribe([
            (FACILITY_TOPIC, 1),
            (MARKET_TOPIC, 1)
        ])
        print(f"✓ Successfully connected to MQTT broker and subscribed to:")
        print(f"   - {FACILITY_TOPIC}")
        print(f"   - {MARKET_TOPIC}\n")
    else:
        print(f"✗ Failed to connect. rc={reason_code}")

def on_message(client, userdata, message):
    """Handle incoming MQTT messages and store latest reading per facility."""
    try:
        payload = json.loads(message.payload.decode())
        if message.topic == FACILITY_TOPIC:
            facility_queue.put(payload)
        elif message.topic == MARKET_TOPIC:
            market_queue.put(payload)

    except Exception as e:
        print(f"on_message error: {e}")

# Run MQTT listener in separate thread
def _mqtt_loop():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 60)
    client.loop_forever()

def start_mqtt_once():
    """Ensure MQTT starts only once."""
    if getattr(start_mqtt_once, "_started", False):
        return
    start_mqtt_once._started = True
    threading.Thread(target=_mqtt_loop, daemon=True).start()

def round_to_bucket(timestamp_str):
    ts = pd.to_datetime(timestamp_str)
    minutes = (ts.minute // TIME_BUCKET_MINUTES) * TIME_BUCKET_MINUTES
    bucket = ts.replace(minute=minutes, second=0, microsecond=0)
    return bucket.isoformat()

def integrate_data_sources():
    """Process queued MQTT messages and integrate with database metadata."""
    global facilities_data, facilities_metadata
    
    facility_processed = 0
    market_processed = 0

    # Process market data first
    while not market_queue.empty():
        try:
            msg = market_queue.get_nowait()
            bucket = round_to_bucket(msg['timestamp'])
            region = msg['region']

            market_buckets[bucket][region] = {
                'price': msg['price'],
                'demand_energy': msg['demand_energy'],
                'timestamp': msg['timestamp']
            }
            market_processed += 1
        except queue.Empty:
            break
        except Exception as e:
            print(f"Error processing market data: {e}")

    # Process facility data
    while not facility_queue.empty():
        try:
            msg = facility_queue.get_nowait()
            facility_code = msg['facility_code']

            # Check if we have metadata for this facility
            if facility_code not in facilities_metadata:
                continue

            metadata = facilities_metadata[facility_code]
            region = metadata['region']

            bucket = round_to_bucket(msg['timestamp'])

            # Try to find matching market data
            market_data = market_buckets.get(bucket, {}).get(region, {})

            if not market_data:
                prev_bucket = (pd.to_datetime(bucket) - 
                              timedelta(minutes=TIME_BUCKET_MINUTES)).isoformat()
                market_data = market_buckets.get(prev_bucket, {}).get(region, {})

            integrated_record = {
                # From MQTT Facilities
                'facility_code': facility_code,
                'power': msg['power'],
                'emissions': msg['emissions'],
                'timestamp': msg['timestamp'],
                'time_bucket': bucket,
                
                # From Database
                'facility_name': metadata['facility_name'],
                'network_region': region,
                'lat': metadata['lat'],
                'lng': metadata['lng'],
                'fuel_type': metadata['fuel_type'],
                
                # From MQTT Market
                'price': market_data.get('price'),
                'demand_energy': market_data.get('demand_energy'),
                'market_timestamp': market_data.get('timestamp'),
                
                # Data quality
                'has_market_data': bool(market_data)
            }
            
            facilities_data[facility_code] = integrated_record
            facility_processed += 1
        
        except queue.Empty:
            break
        except Exception as e:
            print(f"Error integrating facility data: {e}")

    # Only log if data was processed
    if facility_processed > 0 or market_processed > 0:
        print(f"✓ Processed: {facility_processed} facilities, {market_processed} market updates | Total facilities: {len(facilities_data)}")

    return facility_processed, market_processed

def process_queues_continuously():
    """Background thread to continuously process MQTT queues."""
    while True:
        try:
            integrate_data_sources()
            time.sleep(1)  # Process every second
        except Exception as e:
            print(f"Error in queue processing: {e}")
            time.sleep(5)

# Dash App
app = Dash(__name__)
app.title = "Open Electricity"

# Force consistent zoom across localhost/127.0.0.1
app.index_string = """
<!DOCTYPE html>
<html>
  <head>
    {%metas%}
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
    <title>{%title%}</title>
    {%favicon%}
    {%css%}
    <style>
      html, body { margin:0; padding:0; background:#121212; }
      body { zoom: 100%; -webkit-text-size-adjust: 100%;
             font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
      @supports not (zoom: 1) { body { transform: scale(1); transform-origin: 0 0; width: 100%; } }
    </style>
    <script>document.addEventListener('DOMContentLoaded', function(){ try{document.body.style.zoom='100%';}catch(e){} });</script>
  </head>
  <body>
    {%app_entry%}
    <footer>
      {%config%}
      {%scripts%}
      {%renderer%}
    </footer>
  </body>
</html>
"""

# Sidebar UI
sidebar = html.Div(
    [
        html.H1("OPEN ELECTRICITY",
                style={"color":"#fff","fontSize":"28px","margin":"18px 16px 18px","letterSpacing":"0.5px"}),

        html.Label("View Mode:",
                   style={"fontWeight":"bold","margin":"6px 16px","color":"#ffffff","display":"block"}),

        # Switch between 4 display modes
        dcc.RadioItems(
            id="view-mode",
            options=[
                {"label":"Power (MW)","value":"power"},
                {"label":"Emissions (t CO₂)","value":"emissions"},
                {"label":"Demand (MWh)","value":"demand_energy"},
                {"label":"Price ($/MWh)","value":"price"},
            ],
            value="power",
            style={"margin":"0 16px 8px","color":"#bbb"},
        ),

        html.Label("Filter by Network Region",
                   style={"fontWeight":"bold","margin":"10px 16px 6px","color":"#ffffff","display":"block"}),

        # Region filter
        dcc.Dropdown(
            id="region-filter",
            multi=True,
            placeholder="All regions",
            value=[],
            options=[],
            clearable=True,
            style={"margin":"0 5px 10px"}
        ),

        html.Label("Filter by Fuel Type",
                   style={"fontWeight":"bold","margin":"10px 16px 6px","color":"#ffffff","display":"block"}),

        # Fuel filter
        dcc.Dropdown(
            id="fuel-filter",
            multi=True,
            placeholder="All fuel types",
            value=[],
            options=[],
            clearable=True,
            style={"margin":"0 5px 10px"}
        ),

        # Facility count display
        html.Div(id="stats", style={"margin":"8px 16px","color":"#999"}),

        html.Hr(style={"borderColor":"#444","margin":"12px 16px"}),

        html.P("Real-time updates enabled",
               style={"fontSize":"12px","color":"#999","margin":"0 16px 16px"}),
    ],
    style={
        "width":"250px","position":"fixed","height":"100vh","top":"0","left":"0",
        "backgroundColor":"#1e1e1e","overflowY":"auto","zIndex":"1000",
    },
)

# App layout
app.layout = html.Div(
    [
        sidebar,
        html.Div(
            [dcc.Graph(id="facility-map", style={"height":"100vh","width":"100%"},
                       config={"scrollZoom": True})],
            style={"marginLeft":"250px","width":"calc(100% - 250px)","height":"100vh"},
        ),
        # Auto-refresh every 8s
        dcc.Interval(id="interval", interval=8000, n_intervals=0),
        # Store map center & zoom to persist user interactions
        dcc.Store(id="map-state", data={"center":{"lat":-25.5,"lon":134.5}, "zoom":4}),
    ]
)

# Refresh dropdown filter options
@app.callback(
    Output("region-filter","options"),
    Output("fuel-filter","options"),
    Input("interval","n_intervals"),
)
def refresh_filters(_):
    """Update dropdown filter options based on latest data."""
    snap = dict(facilities_data)
    regions, fuels = set(), set()
    for r in snap.values():
        if r.get("network_region"): regions.add(r["network_region"])
        if r.get("fuel_type"): fuels.add(r["fuel_type"])
    region_opts = [{"label":x,"value":x} for x in sorted(regions)]
    fuel_opts = [{"label":x,"value":x} for x in sorted(fuels)]
    return region_opts, fuel_opts

# Main map update logic
@app.callback(
    Output("facility-map","figure"),
    Output("stats","children"),
    Output("map-state","data"),
    Input("interval","n_intervals"),
    Input("view-mode","value"),
    Input("region-filter","value"),
    Input("fuel-filter","value"),
    State("facility-map","relayoutData"),
    State("map-state","data"),
)
def update_map(_, view_mode, region_sel, fuel_sel, relayout, map_state):
    """Render map with markers sized by selected view mode."""
    # Preserve user pan/zoom
    if relayout:
        if "mapbox.center" in relayout and isinstance(relayout["mapbox.center"], dict):
            map_state["center"] = relayout["mapbox.center"]
        if "mapbox.zoom" in relayout:
            map_state["zoom"] = relayout["mapbox.zoom"]

    region_set = set(region_sel or [])
    fuel_set = set(fuel_sel or [])

    snap = dict(facilities_data)

    # Determine latest price/demand per region
    latest_by_region = {}
    for r in snap.values():
        reg = r.get("network_region")
        if not reg:
            continue
        ts = parse_ts(r.get("timestamp"))
        prev = latest_by_region.get(reg)
        if (prev is None) or (ts > prev["ts"]):
            latest_by_region[reg] = {
                "price": r.get("price") or 0.0,
                "demand": r.get("demand_energy") or 0.0,
                "ts": ts,
            }

    # Filter facilities
    rows = []
    for r in snap.values():
        if not r.get("lat") or not r.get("lng"):
            continue
        if region_set and r.get("network_region") not in region_set:
            continue
        if fuel_set and r.get("fuel_type") not in fuel_set:
            continue
        rows.append(r)

    # Determine which metric controls marker size
    def metric_value(r):
        reg = r.get("network_region")
        if view_mode == "power":
            return float(r.get("power") or 0.0)
        elif view_mode == "emissions":
            return float(r.get("emissions") or 0.0)
        elif view_mode == "demand_energy":
            return float(latest_by_region.get(reg, {}).get("demand", 0.0))
        else:  # price
            return float(latest_by_region.get(reg, {}).get("price", 0.0))

    # Scale marker sizes
    MIN_SIZE, MAX_SIZE = 15, 30
    values_for_scale = [metric_value(r) for r in rows] or [0.0]
    vmin, vmax = min(values_for_scale), max(values_for_scale)
    def scale(v):
        if vmax <= vmin:
            return (MIN_SIZE + MAX_SIZE) / 2
        return MIN_SIZE + (v - vmin) * (MAX_SIZE - MIN_SIZE) / (vmax - vmin)

    # Sidebar stats
    stats = html.Div(
        [
            html.H2(str(len(rows)),
                    style={"margin":"0","color":"#fff","fontSize":"28px","fontWeight":"bold"}),
            html.P("FACILITIES",
                   style={"margin":"2px 0 0","color":"#999","fontSize":"12px","textTransform":"uppercase","letterSpacing":"1px"}),
        ]
    )

    # Group markers by fuel type (for legend)
    by_fuel = defaultdict(list)
    for r in rows:
        by_fuel[r.get("fuel_type") or "Other"].append(r)

    # Build figure
    fig = go.Figure()
    for fuel, items in by_fuel.items():
        sizes = [scale(metric_value(r)) for r in items]

        # Hover details
        texts = []
        for r in items:
            reg = r.get("network_region") or "-"
            reg_stats = latest_by_region.get(reg, {"price":0.0, "demand":0.0, "ts": datetime.min})
            texts.append(
                f"<b>{r.get('facility_name') or r['facility_code']}</b><br>"
                f"Code: {r['facility_code']}<br>"
                f"Region: {reg}<br>"
                f"Fuel: {fuel}<br>"
                f"Power: {fmt2(r.get('power'))} MW<br>"
                f"Emissions: {fmt2(r.get('emissions'))} t<br>"
                f"<b>Regional price ({reg}):</b> {fmt2(reg_stats['price'])} $/MWh<br>"
                f"<b>Regional demand ({reg}):</b> {fmt2(reg_stats['demand'])} MWh<br>"
                f"Updated: {r.get('timestamp') or '-'}"
            )

        fig.add_trace(
            go.Scattermapbox(
                lat=[r["lat"] for r in items],
                lon=[r["lng"] for r in items],
                mode="markers",
                marker=dict(
                    size=sizes,
                    color=FUEL_COLORS.get(fuel, FUEL_COLORS["Other"]),
                    opacity=0.85,
                ),
                text=texts,
                hovertemplate="%{text}<extra></extra>",
                name=fuel,
            )
        )

    # Layout and legend
    fig.update_layout(
        mapbox=dict(style="open-street-map",
                    center=map_state["center"], zoom=map_state["zoom"]),
        margin=dict(l=0, r=0, t=0, b=0),
        hovermode="closest",
        uirevision="keep",
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        legend=dict(
            title=None,
            bgcolor="rgba(255,255,255,0.90)",
            bordercolor="#d1d5db",
            borderwidth=1,
            orientation="v",
            x=0.01, y=0.01,
            xanchor="left", yanchor="bottom",
            itemsizing="constant",
            itemclick=False,
            itemdoubleclick=False,
            font=dict(size=11),
        ),
    )

    return fig, stats, map_state

# Run app
if __name__ == "__main__":
    # Load metadata once at startup
    print("Loading facility metadata from database...")
    facilities_metadata = get_data_from_db()
    
    # Start MQTT connection
    start_mqtt_once()
    
    # Start background queue processor
    print("Starting background data processor...")
    threading.Thread(target=process_queues_continuously, daemon=True).start()
    
    print("\n✓ Dashboard starting on http://0.0.0.0:8051\n")
    app.run(host="0.0.0.0", port=8051, debug=False, use_reloader=False)