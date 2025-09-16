#!/usr/bin/env python3
import asyncio
import sqlite3
import json
import socket

import paho.mqtt.client as mqtt

UDP_PORT = 4401
DB_FILE = "netstats.db"

# sqlite connect
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()
c.execute("""
CREATE TABLE IF NOT EXISTS minute_stats (
  agent_id TEXT NOT NULL,
  minute_utc TEXT NOT NULL,
  latency_min_ms REAL NOT NULL,
  latency_max_ms REAL NOT NULL,
  latency_avg_ms REAL NOT NULL,
  jitter_min_ms REAL NOT NULL,
  jitter_max_ms REAL NOT NULL,
  jitter_avg_ms REAL NOT NULL,
  sent INTEGER NOT NULL,
  received INTEGER NOT NULL,
  lost INTEGER NOT NULL,
  PRIMARY KEY (agent_id, minute_utc)
);
""")
conn.commit()

def upsert_stats(data):
    c.execute("""
    INSERT INTO minute_stats VALUES (?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(agent_id, minute_utc) DO UPDATE SET
    latency_min_ms=excluded.latency_min_ms,
    latency_max_ms=excluded.latency_max_ms,
    latency_avg_ms=excluded.latency_avg_ms,
    jitter_min_ms=excluded.jitter_min_ms,
    jitter_max_ms=excluded.jitter_max_ms,
    jitter_avg_ms=excluded.jitter_avg_ms,
    sent=excluded.sent,
    received=excluded.received,
    lost=excluded.lost
    """, (
        data["agent_id"], data["time"], data["latency_min_ms"], data["latency_max_ms"], data["latency_avg_ms"],
        data["jitter_min_ms"], data["jitter_max_ms"], data["jitter_avg_ms"], data["sent"], data["received"], data["lost"]
    ))
    conn.commit()

# mqtt
def on_connect(client, userdata, flags, rc):
    print("MQTT connected:", rc)
    client.subscribe("netstats/+/minute")
    
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        upsert_stats(data)
        print(f"Stored stats for agent {data['agent_id']} at {data['time']}")
    except Exception as e:
        print("Error handling message:", e)

MQTT_CLIENT = mqtt.Client()
MQTT_CLIENT.on_connect = on_connect
MQTT_CLIENT.on_message = on_message
MQTT_CLIENT.connect("localhost", 1883)

async def mqtt_loop():
    while True:
        MQTT_CLIENT.loop_read()
        MQTT_CLIENT.loop_write()
        MQTT_CLIENT.loop_misc()
        await asyncio.sleep(0.1)

# UDP echo server
async def udp_echo_server():
    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    sock.bind(('0.0.0.0', UDP_PORT))
    
    print(f"UDP echo server listening on port {UDP_PORT}")
    
    while True:
        try:
            # Receive packet
            data, addr = await asyncio.get_event_loop().sock_recvfrom(sock, 4096)
            
            # Parse and log
            packet = json.loads(data.decode())
            print(f"Agent B received from {addr}: seq={packet['seq']}")
            
            # Echo back immediately with print
            await asyncio.get_event_loop().sock_sendto(sock, data, addr)
            print(f"Agent B echoed back: seq={packet['seq']}")
            
        except Exception as e:
            print(f"Error in UDP echo server: {e}")
            await asyncio.sleep(0.01)

async def main():
    await asyncio.gather(
        udp_echo_server(),
        mqtt_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())