import pandas as pd
import json
from cassandra.cluster import Cluster

def run_analytics():
    print("Connecting to Cassandra to fetch live telemetry...")
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect('snap_analytics')

    # Pull the massive dataset into memory
    print("Extracting data...")
    query = "SELECT match_id, event_type, event_data FROM match_events"
    rows = session.execute(query)

    # Transform the raw rows and nested JSON into a flat, structured format
    data = []
    for row in rows:
        event_dict = json.loads(row.event_data) if row.event_data else {}
        data.append({
            'match_id': row.match_id,
            'event_type': row.event_type,
            **event_dict  # Unpack the JSON properties into distinct columns
        })

    