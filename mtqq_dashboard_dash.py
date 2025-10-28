import json
import threading
import paho.mqtt.client as mqtt
from dash import Dash, html, dcc, Input, Output, State
import plotly.graph_objects as go
from datetime import datetime

# MQTT Config
BROKER = "localhost"
PORT = 1883
TOPIC = "COMP5339/T07G04/facilities"

# Global data storage (thread-safe dict operations in Python)
facilities_data = {}
mqtt_connected = False

# MQTT Callbacks
def on_connect(client, userdata, flags, reason_code, properties=None):
    global mqtt_connected
    if reason_code == 0:
        mqtt_connected = True
        client.subscribe(TOPIC)
        print(f"Connected to MQTT broker and subscribed to {TOPIC}")
    else:
        mqtt_connected = False
        print(f"Failed to connect. Return code: {reason_code}")

def on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode())
        facility_code = payload.get("facility_code")
        
        facilities_data[facility_code] = {
            "facility_code": facility_code,
            "facility_name": payload.get("facility_name"),
            "lat": payload.get("lat"),
            "lng": payload.get("lng"),
            "power": payload.get("power"),
            "emissions": payload.get("emissions"),
            "timestamp": payload.get("timestamp")
        }
        print(f"Received: {facility_code} - {len(facilities_data)} total facilities")
    except Exception as e:
        print(f"Error processing message: {e}")

# Start MQTT in background thread
def start_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(BROKER, PORT, 60)
        client.loop_forever()
    except Exception as e:
        print(f"MQTT error: {e}")

mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
mqtt_thread.start()

# Initialize Dash app with dark theme
app = Dash(__name__)

# Set dark theme for the app
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                background-color: #121212;
                margin: 0;
                padding: 0;
                font-family: 'Avenir', 'Avenir Next', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            }
            * {
                font-family: 'Avenir', 'Avenir Next', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            }
        </style>
        <link href="https://fonts.googleapis.com/css?family=Avenir" rel="stylesheet">
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
'''

# App layout
app.layout = html.Div([
    # Sidebar
    html.Div([
        html.H2("Dashboard", 
                style={'textAlign': 'center', 'color': '#ffffff', 'marginBottom': '30px', 'marginTop': '20px'}),
        
        # Connection status
        html.Div(id='connection-status', style={'marginBottom': '30px', 'padding': '0 20px'}),
        
        # View mode selector
        html.Label('View Mode:', style={'fontWeight': 'bold', 'marginBottom': '15px', 'color': '#ffffff', 'display': 'block', 'padding': '0 20px'}),
        dcc.RadioItems(
            id='view-mode',
            options=[
                {'label': 'Power (MW)', 'value': 'power'},
                {'label': 'Emissions (t CO₂)', 'value': 'emissions'}
            ],
            value='power',
            style={'marginBottom': '30px', 'padding': '0 20px', 'color': '#999'}
        ),
        
        # Stats
        html.Div(id='stats', style={'marginBottom': '30px', 'padding': '0 20px'}),
        
        html.Hr(style={'borderColor': '#555', 'margin': '30px 20px'}),
        
        html.P('Real-time updates enabled', 
               style={'fontSize': '12px', 'color': '#999', 'textAlign': 'center'}),
    ], style={
        'width': '250px',
        'position': 'fixed',
        'height': '100vh',
        'top': '0',
        'left': '0',
        'backgroundColor': '#1e1e1e',
        'overflowY': 'auto',
        'zIndex': '1000'
    }),
    
    # Map container
    html.Div([
        dcc.Graph(
            id='facility-map',
            style={'height': '100vh', 'width': '100%'},
            config={'scrollZoom': True}
        )
    ], style={
        'marginLeft': '250px',
        'width': 'calc(100% - 250px)',
        'height': '100vh'
    }),
    
    # Interval component for updates (every 5 seconds)
    dcc.Interval(
        id='interval-component',
        interval=10000,  # 5 seconds
        n_intervals=0
    ),
    
    # Store for map state
    dcc.Store(id='map-state', data={'center': {'lat': -25.5, 'lon': 134.5}, 'zoom': 4})
])

# Callback to update everything
@app.callback(
    [Output('facility-map', 'figure'),
     Output('connection-status', 'children'),
     Output('stats', 'children'),
     Output('map-state', 'data')],
    [Input('interval-component', 'n_intervals'),
     Input('view-mode', 'value')],
    [State('facility-map', 'relayoutData'),
     State('map-state', 'data')]
)
def update_dashboard(n, view_mode, relayout_data, map_state):
    # Update map state from user interaction
    if relayout_data:
        if 'mapbox.center' in relayout_data:
            center = relayout_data['mapbox.center']
            if isinstance(center, dict):
                map_state['center'] = center
        if 'mapbox.zoom' in relayout_data:
            map_state['zoom'] = relayout_data['mapbox.zoom']
    
    # Connection status
    if mqtt_connected:
        status = html.Div([
            html.Div([
                html.P("Connected", style={'margin': '0', 'fontWeight': 'bold', 'fontSize': '14px'}),
                html.P(f"to {TOPIC}", style={'margin': '2px 0 0 0', 'fontSize': '12px', 'opacity': '0.8'})
            ], style={'flex': '1'})
        ], style={
            'display': 'flex',
            'alignItems': 'center',
            'gap': '8px',
            'backgroundColor': '#065f46',
            'color': '#10b981',
            'padding': '8px 12px',
            'borderRadius': '6px',
            'border': '1px solid #10b981'
        })
    else:
        status = html.Div([
            html.Div([
                html.P("Connecting...", style={'margin': '0', 'fontWeight': 'bold', 'fontSize': '14px'}),
                html.P("Attempting to connect", style={'margin': '2px 0 0 0', 'fontSize': '12px', 'opacity': '0.8'})
            ], style={'flex': '1'})
        ], style={
            'display': 'flex',
            'alignItems': 'center',
            'gap': '8px',
            'backgroundColor': '#78350f',
            'color': '#f59e0b',
            'padding': '8px 12px',
            'borderRadius': '6px',
            'border': '1px solid #f59e0b'
        })
    
    # Create a snapshot of facilities_data to avoid "dictionary changed size" error
    # during iteration when MQTT updates occur
    facilities_snapshot = dict(facilities_data)
    
    # Stats
    num_facilities = len(facilities_snapshot)
    stats = html.Div([
        html.H2(str(num_facilities), style={'margin': '0', 'color': '#ffffff', 'fontSize': '28px', 'fontWeight': 'bold'}),
        html.P('Facilities', style={'margin': '5px 0 0 0', 'color': '#999', 'fontSize': '12px', 'textTransform': 'uppercase', 'letterSpacing': '1px'})
    ])
    
    # Create map figure with efficient batch markers
    fig = go.Figure()
    
    # Group markers by size for efficient rendering
    marker_groups = {}
    
    for facility_code, data in facilities_snapshot.items():
        val = data['power'] if view_mode == 'power' else data['emissions']
        
        # Calculate marker size
        if val > 0:
            size = min(max(val / 10, 5), 30)
        else:
            size = 5
        
        # Round size to reduce number of traces
        size_key = round(size / 2) * 2  # Round to even numbers
        
        if size_key not in marker_groups:
            marker_groups[size_key] = {'lats': [], 'lons': [], 'texts': [], 'size': size_key}
        
        hover_text = (
            f"<b>{data['facility_name']}</b><br>"
            f"Code: {data['facility_code']}<br>"
            f"Power: {data['power']} MW<br>"
            f"Emissions: {data['emissions']} t<br>"
            f"Updated: {data['timestamp']}"
        )
        
        marker_groups[size_key]['lats'].append(data['lat'])
        marker_groups[size_key]['lons'].append(data['lng'])
        marker_groups[size_key]['texts'].append(hover_text)
    
    # Add traces for each size group (much more efficient than individual traces)
    for size_key, group_data in marker_groups.items():
        fig.add_trace(go.Scattermapbox(
            lat=group_data['lats'],
            lon=group_data['lons'],
            mode='markers',
            marker=dict(
                size=group_data['size'],
                color='#FF8C00',
                opacity=0.8
            ),
            text=group_data['texts'],
            hovertemplate='%{text}<extra></extra>',
            showlegend=False
        ))
    
    # Update map layout with preserved center and zoom
    fig.update_layout(
        mapbox=dict(
            style='open-street-map',  # Light theme map
            center=map_state['center'],
            zoom=map_state['zoom']
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        hovermode='closest',
        uirevision='constant',  # This preserves the map state!
        paper_bgcolor='#ffffff',  # Light background
        plot_bgcolor='#ffffff'   # Light plot background
    )
    
    return fig, status, stats, map_state

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8050)