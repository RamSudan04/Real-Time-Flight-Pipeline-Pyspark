import time
import json
import stomp
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

# Configuration for Project 2 Ports
ACTIVEMQ_HOST = 'localhost'
ACTIVEMQ_PORT = 61614
QUEUE_NAME = 'processed_flight_data'  # Listening to PySpark's output queue

INFLUXDB_URL = 'http://localhost:8087'  # Project 2 InfluxDB Port
INFLUXDB_TOKEN = 'pyspark-secret-token'
INFLUXDB_ORG = 'internship'
INFLUXDB_BUCKET = 'pyspark_data'

# Initialize InfluxDB Client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

class ProcessedDataListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_message(self, frame):
        try:
            # Parse the processed data sent by PySpark
            data = json.loads(frame.body)
            
            # Construct InfluxDB time-series data point
            point = Point("flight_telemetry") \
                .tag("flight_status", data["flight_status"]) \
                .field("speed_kmh", float(data["speed"])) \
                .field("speed_knots", float(data["speed_knots"])) \
                .field("altitude", float(data["altitude"])) \
                .field("roll", float(data["roll"])) \
                .time(data["timestamp"] * 1000000) # Convert ms to ns timestamp
            
            # Write to InfluxDB bucket
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            print(f" [InfluxDB Saved] -> Status: {data['flight_status']} | Speed: {data['speed_knots']} knots")
            
        except Exception as e:
            print(f"Error processing message: {e}")

def main():
    conn = stomp.Connection([(ACTIVEMQ_HOST, ACTIVEMQ_PORT)])
    conn.set_listener('', ProcessedDataListener(conn))
    conn.connect('admin', 'admin', wait=True)
    
    # Subscribe to the PROCESSED data queue
    conn.subscribe(destination=f"/queue/{QUEUE_NAME}", id=2, ack='auto')
    
    print(f"📥 Consumer started. Listening to '{QUEUE_NAME}' and saving to InfluxDB on port 8087...")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Consumer...")
    finally:
        conn.disconnect()
        client.close()

if __name__ == '__main__':
    main()