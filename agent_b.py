#!/usr/bin/env python3
import asyncio, sqlite3, json
import paho.mqtt.client as mqtt

TCP_PORT = 4401
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

def upsert_stats(msg):
    data = json.loads(msg)
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
MQTT_CLIENT = mqtt.Client()
MQTT_CLIENT.on_message = lambda client, userdata, msg: upsert_stats(msg.payload)
MQTT_CLIENT.connect("localhost",1885)
MQTT_CLIENT.subscribe("netstats/+/minute")
MQTT_CLIENT.loop_start()

# tcp echo read write back
async def handle(reader, writer):
    while True:
        line = await reader.readline()
        if not line:
            break
        print("Agent B received:", line.decode().strip())
        writer.write(line)
        await writer.drain()
        print("Agent B echoed back:", line.decode().strip())
    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle, '0.0.0.0', TCP_PORT)
    async with server:
        await server.serve_forever()

asyncio.run(main())
