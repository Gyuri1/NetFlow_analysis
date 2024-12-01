import pandas as pd
import plotly.graph_objs as go
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import numpy as np




# Read the CSV data
#data = "https://raw.githubusercontent.com/Gyuri1/NetFlow_analysis/refs/heads/main/netflow-small.csv"


# Read the CSV data
data = "https://raw.githubusercontent.com/Gyuri1/NetFlow_analysis/refs/heads/main/netflow-large.csv"


# Data Loading and Preprocessing Function
def load_and_process_data(csv_path):
    # Load the CSV file
    df = pd.read_csv(csv_path, parse_dates=['Start'])
    
    # Process bytes and host groups
    df['Subject Bytes'] = df['Subject Bytes'].apply(safe_convert_bytes)
    df['Peer Bytes'] = df['Peer Bytes'].apply(safe_convert_bytes)


    # Convert Total Bytes to numeric
    df['Total Bytes'] = df['Total Bytes'].apply(safe_convert_bytes)
    
    return df



# Safe byte conversion function
def safe_convert_bytes(byte_str):
    if not isinstance(byte_str, str):
        return 0.0
    
    byte_str = byte_str.strip().upper()
    
    if not byte_str or byte_str in ['', '-', '--']:
        return 0.0
    
    byte_str = byte_str.replace(' ', '')
    
    try:
        if byte_str.endswith('K'):
            return float(byte_str[:-1]) * 1024
        elif byte_str.endswith('M'):
            return float(byte_str[:-1]) * 1024 * 1024
        elif byte_str.endswith('G'):
            return float(byte_str[:-1]) * 1024 * 1024 * 1024
        else:
            return float(byte_str)
    except (ValueError, TypeError):
        print(f"Warning: Could not convert '{byte_str}' to numeric value")
        return 0.0


def preprocess_data(df):
    """
    Preprocess the dataframe for visualization
    """
    # Convert bytes columns
    def convert_bytes(byte_str):
        byte_str = str(byte_str).replace(' M', '').replace(' K', '')
        try:
            return float(byte_str)
        except:
            return 0.0

    df['Total Bytes'] = df['Total Bytes'].apply(convert_bytes)
    df['Start'] = pd.to_datetime(df['Start'])
    
    return df

def aggregate_host_group_connections(df, top_n=10):
    """
    Aggregate connections between host groups
    """
    # Aggregate connections between host groups
    connection_df = df.groupby([
        'Subject Host Groups', 
        'Peer Host Groups'
    ])['Total Bytes'].sum().reset_index()
    
    # Normalize host group names (split and clean)
    connection_df['Subject Host Groups'] = connection_df['Subject Host Groups'].apply(lambda x: x.split(',')[0].strip())
    connection_df['Peer Host Groups'] = connection_df['Peer Host Groups'].apply(lambda x: x.split(',')[0].strip())
    
    # Get top N connections
    top_connections = connection_df.nlargest(top_n, 'Total Bytes')
    
    return top_connections

def create_dash_app(df):
    """
    Create Dash application for network flow visualization
    """
    app = dash.Dash(__name__)
    
    # Preprocessing
    df = preprocess_data(df)
    
    app.layout = html.Div([
        html.H1('Network Flow Host Group Connections', 
                style={'textAlign': 'center', 'color': '#1E90FF'}),
        
        html.Div([
            html.Label('Top N Connections:', 
                       style={'marginRight': '10px'}),
            dcc.Slider(
                id='top-n-slider',
                min=3,
                max=20,
                value=10,
                marks={i: str(i) for i in range(3, 21, 2)},
                step=1
            )
        ], style={'width': '50%', 'margin': 'auto'}),
        
        html.Div([
            html.Label('Date Range:'),
            dcc.DatePickerRange(
                id='date-picker-range',
                min_date_allowed=df['Start'].min(),
                max_date_allowed=df['Start'].max(),
                initial_visible_month=df['Start'].min(),
                start_date=df['Start'].min(),
                end_date=df['Start'].max()
            )
        ], style={'textAlign': 'center', 'margin': '20px'}),
        
        dcc.Graph(id='network-bubble-graph')
    ])
    
    @app.callback(
        Output('network-bubble-graph', 'figure'),
        [Input('date-picker-range', 'start_date'),
         Input('date-picker-range', 'end_date'),
         Input('top-n-slider', 'value')]
    )
    def update_bubble_graph(start_date, end_date, top_n):
        # Filter data by date range
        filtered_df = df[
            (df['Start'] >= start_date) & 
            (df['Start'] <= end_date)
        ]
        
        # Create network data
        connections = aggregate_host_group_connections(filtered_df, top_n)
        
        # Prepare data for bubble graph
        unique_groups = set(connections['Subject Host Groups'].tolist() + 
                            connections['Peer Host Groups'].tolist())
        
        # Create positions for bubbles in a circular layout
        import math
        num_groups = len(unique_groups)
        group_positions = {}
        for i, group in enumerate(unique_groups):
            angle = 2 * math.pi * i / num_groups
            radius = 1
            group_positions[group] = {
                'x': radius * math.cos(angle),
                'y': radius * math.sin(angle)
            }
        
        # Prepare bubble traces
        bubble_traces = []
        edge_traces = []
        
        # Bubble trace
        for group, pos in group_positions.items():
            # Calculate bubble size based on total connections
            group_total_bytes = connections[
                (connections['Subject Host Groups'] == group) | 
                (connections['Peer Host Groups'] == group)
            ]['Total Bytes'].sum()
            
            #bubble_size = np.log(group_total_bytes + 1) * 10  # Log scale for size
            bubble_size = np.log(group_total_bytes + 1) * 2  # Log scale for size
            
            bubble_traces.append(go.Scatter(
                x=[pos['x']],
                y=[pos['y']],
                mode='markers+text',
                marker=dict(
                    size=bubble_size,
                    color='rgba(50, 171, 96, 0.7)',
                    line=dict(color='rgba(50, 171, 96, 1)', width=2)
                ),
                text=[group],
                textposition='bottom center'
            ))
        
        # Connection traces
        for _, row in connections.iterrows():
            start = group_positions[row['Subject Host Groups']]
            end = group_positions[row['Peer Host Groups']]
            
            # Line thickness based on total bytes
            line_width = np.log(row['Total Bytes'] + 1)
            
            edge_traces.append(go.Scatter(
                x=[start['x'], end['x']],
                y=[start['y'], end['y']],
                mode='lines',
                line=dict(
                    width=line_width,
                    color='rgba(100, 100, 100, 0.5)'
                ),
                hovertext=f"{row['Subject Host Groups']} → {row['Peer Host Groups']}<br>Bytes: {row['Total Bytes']:.2f}"
            ))
        
        # Combine traces
        fig_data = edge_traces + bubble_traces
        
        # Layout
        layout = go.Layout(
            title='Host Group Connections',
            showlegend=False,
            hovermode='closest',
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
        
        return {
            'data': fig_data,
            'layout': layout
        }
    
    return app

# Demonstration mode (replace with actual data loading)
if __name__ == '__main__':
    # Simulated data loading
    """
    data = {
        'Start': ['2024-11-25T19:10:12.000+0000', '2024-11-25T19:10:44.000+0000'],
        'Subject Host Groups': ['End User Devices, Atlanta', 'End User Devices, Sales'],
        'Peer Host Groups': ['Atlanta, Protected', 'Casablanca, QA'],
        'Total Bytes': ['168.67 M', '118.86 M']
    }
    """
    # Read the CSV data
    df = load_and_process_data(data)

    #df = pd.DataFrame(data)
    
    app = create_dash_app(df)
    app.run_server(debug=True)