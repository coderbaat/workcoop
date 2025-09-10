#!/usr/bin/env python3
import asyncio, json, uuid, os, time
import paho.mqtt.client as mqtt
from datetime import datetime, timezone

STATE_DIR = os.environ.get("AGENT_STATE_DIR", os.path.expanduser("~/.agent_a"))
ID_FILE = os.path.join(STATE_DIR, "id")
TCP_HOST = "localhost"
TCP_PORT = 4401
RATE = 2  # probes/sec
TIMEOUT_S = 2

#UUID
os.makedirs(STATE_DIR, exist_ok=True)
if os.path.exists(ID_FILE):
    with open(ID_FILE) as f:
        AGENT_ID = f.read().strip()
else:
    AGENT_ID = str(uuid.uuid4())
    with open(ID_FILE, "w") as f:
        f.write(AGENT_ID)

# mqtt
MQTT_CLIENT = mqtt.Client()
MQTT_CLIENT.connect("localhost", 1885)
MQTT_CLIENT.loop_start()

# metrics
in_flight = {}  # seq to t_send_ns
seq = 0
window_acc = []
last_window_minute = None

def compute_stats(records):
    if not records:
        return None
    rtts = [r['rtt'] for r in records]
    jitters = [abs(rtts[i]-rtts[i-1]) for i in range(1,len(rtts))] or [0]
    stats = {
        "latency_min_ms": min(rtts),
        "latency_max_ms": max(rtts),
        "latency_avg_ms": sum(rtts)/len(rtts),
        "jitter_min_ms": min(jitters),
        "jitter_max_ms": max(jitters),
        "jitter_avg_ms": sum(jitters)/len(jitters),
        "sent": len(records)+sum(r.get('lost',0) for r in records),
        "received": len(records),
        "lost": sum(r.get('lost',0) for r in records)
    }
    print("compute_stats() called →", stats)   # 👈 debug line
    return stats

async def tcp_probe():
    global seq, in_flight, window_acc, last_window_minute
    backoff = 0.5
    while True:
        try:
            reader, writer = await asyncio.open_connection(TCP_HOST, TCP_PORT)
            backoff = 0.5
            print("TCP connected")
            break
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff*2, 5)

    async def send_loop():
        global seq
        interval = 1/RATE
        while True:
            t_send_ns = time.monotonic_ns()
            packet = {"agent_id": AGENT_ID, "seq": seq, "t_send_ns": t_send_ns}
            writer.write((json.dumps(packet)+"\n").encode())
            await writer.drain()
            in_flight[seq] = t_send_ns
            seq = (seq + 1) % 65536
            await asyncio.sleep(interval)

    async def recv_loop():
        global last_window_minute, window_acc
        while True:
            line = await reader.readline()
            if not line:
                break
            try:
                data = json.loads(line)
                s = data["seq"]
                t_send = in_flight.pop(s, None)
                if t_send is None:
                    continue
                rtt_ms = (time.monotonic_ns() - t_send)/1e6
                print(f"Received echo: seq={s}, RTT={rtt_ms:.2f} ms, raw={line.decode().strip()}")
                now = datetime.now(timezone.utc)
                minute = now.replace(second=0,microsecond=0)
                if last_window_minute and minute > last_window_minute:
                    # finalize previous minute
                    stats = compute_stats(window_acc)
                    if stats:
                        stats_msg = {"agent_id": AGENT_ID, "time": last_window_minute.isoformat().replace("+00:00","Z"), **stats}
                        MQTT_CLIENT.publish(f"netstats/{AGENT_ID}/minute", json.dumps(stats_msg))
                        print("Published stats:", stats_msg)
                    window_acc = []
                last_window_minute = minute
                window_acc.append({"rtt": rtt_ms})
            except Exception:
                continue

    async def timeout_sweep():
        global window_acc
        while True:
            now_ns = time.monotonic_ns()
            lost_seqs = [s for s,t in in_flight.items() if now_ns-t>TIMEOUT_S*1e9]
            for s in lost_seqs:
                in_flight.pop(s)
                window_acc.append({"rtt": 0.0, "lost":1})
            await asyncio.sleep(0.5)

    await asyncio.gather(send_loop(), recv_loop(), timeout_sweep())

asyncio.run(tcp_probe())