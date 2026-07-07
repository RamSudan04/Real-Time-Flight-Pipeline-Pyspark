import time
import json
import random
import stomp

# Configuration for the new project ports
ACTIVEMQ_HOST = 'localhost'
ACTIVEMQ_PORT = 61614  # Isolated STOMP port for Project 2
QUEUE_NAME = 'raw_flight_data'  # This goes to the raw queue for PySpark

def generate_telemetry():
    return {
        "speed": round(random.uniform(200.0, 800.0), 2),
        "altitude": round(random.uniform(10000.0, 35000.0), 2),
        "roll": round(random.uniform(-45.0, 45.0), 2),
        "timestamp": int(time.time() * 1000)  # Millisecond timestamp
    }

def main():
    # Establish connection to ActiveMQ
    conn = stomp.Connection([(ACTIVEMQ_HOST, ACTIVEMQ_PORT)])
    conn.connect('admin', 'admin', wait=True)
    
    print(f"🚀 Producer started. Sending telemetry to ActiveMQ queue: '{QUEUE_NAME}' on port {ACTIVEMQ_PORT}...")
    
    try:
        while True:
            data = generate_telemetry()
            json_payload = json.dumps(data)
            
            # Publish to the raw data queue
            conn.send(body=json_payload, destination=f"/queue/{QUEUE_NAME}")
            print(f" [x] Sent: {json_payload}")
            
            time.sleep(1)  # Generate data every second
            
    except KeyboardInterrupt:
        print("\nStopping Producer...")
    finally:
        conn.disconnect()

if __name__ == '__main__':
    main()