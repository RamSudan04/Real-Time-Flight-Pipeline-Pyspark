import time
import json
import stomp
import os
import sys
import gc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round

# --- THE FIX: Forcing strict IPv4 communication in Windows ---
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configuration for Project 2
ACTIVEMQ_HOST = '127.0.0.1' # Switched from 'localhost' to strict IP
ACTIVEMQ_PORT = 61614
INPUT_QUEUE = 'raw_flight_data'
OUTPUT_QUEUE = 'processed_flight_data'

message_buffer = []

# 1. Initialize PySpark Session
print("Initializing PySpark Engine (this takes a few seconds)...")
spark = SparkSession.builder \
    .appName("FlightTelemetryProcessor") \
    .master("local[*]") \
    .config("spark.network.timeout", "600s") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

class SparkProcessorListener(stomp.ConnectionListener):
    def on_message(self, frame):
        try:
            raw_data = json.loads(frame.body)
            message_buffer.append(raw_data)
        except Exception as e:
            print(f"Error reading message: {e}")

def main():
    # CONNECTION 1: Dedicated to LISTENING
    conn_receiver = stomp.Connection([(ACTIVEMQ_HOST, ACTIVEMQ_PORT)])
    conn_receiver.set_listener('', SparkProcessorListener())
    conn_receiver.connect('admin', 'admin', wait=True)
    conn_receiver.subscribe(destination=f"/queue/{INPUT_QUEUE}", id=1, ack='auto')
    
    # CONNECTION 2: Dedicated to SENDING
    conn_sender = stomp.Connection([(ACTIVEMQ_HOST, ACTIVEMQ_PORT)])
    conn_sender.connect('admin', 'admin', wait=True)
    
    print(f"⚡ PySpark Stream Processor listening to '{INPUT_QUEUE}'...")
    print(f"⚡ Processed data will be sent to '{OUTPUT_QUEUE}'...")
    
    try:
        while True:
            time.sleep(1.0) 
            
            if len(message_buffer) > 0:
                batch = message_buffer.copy()
                message_buffer.clear()
                
                try:
                    df = spark.createDataFrame(batch)
                    
                    processed_df = df.withColumn("speed_knots", round(col("speed") * 0.539957, 2)) \
                                     .withColumn("flight_status", 
                                                 when(col("altitude") < 15000, "ASCENDING/DESCENDING")
                                                 .otherwise("CRUISING"))
                    
                    processed_rows = [row.asDict() for row in processed_df.collect()]
                    
                    for row_data in processed_rows:
                        json_payload = json.dumps(row_data)
                        conn_sender.send(body=json_payload, destination=f"/queue/{OUTPUT_QUEUE}")
                        print(f" [PySpark Processed] -> {json_payload}")
                    
                    # Memory Cleanup
                    del df
                    del processed_df
                    del batch
                    del processed_rows
                    gc.collect() 
                        
                except Exception as e:
                    print(f"Error during PySpark processing: {e}")
            
    except KeyboardInterrupt:
        print("\nStopping PySpark Processor...")
    finally:
        conn_receiver.disconnect()
        conn_sender.disconnect()
        spark.stop()

if __name__ == '__main__':
    main()