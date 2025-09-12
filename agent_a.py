import asyncio
import json
import uuid
import os
import time
from datetime import datetime, timezone, timedelta

#!/usr/bin/env python3
import paho.mqtt.client as mqtt

STATE_DIR = os.environ.get("AGENT_STATE_DIR", os.path.expanduser("~/.agent_a"))
ID_FILE = os.path.join(STATE_DIR, "id")
TCP_HOST = "localhost"
TCP_PORT = 4401
RATE = 2  # probes/sec
TIMEOUT_S = 2  

# UUID
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
MQTT_CLIENT.connect("localhost", 1885)  # Changed to standard MQTT port
MQTT_CLIENT.loop_start()

# metrics
in_flight = {}  # seq to t_send_ns
seq = 0
current_window = {"data": [], "minute": None}
pending_window = {"data": [], "minute": None}

def compute_stats(records):
    if not records:
        return None
    
    valid_rtts = [r['rtt'] for r in records if 'rtt' in r and r['rtt'] > 0]
    lost_count = sum(1 for r in records if r.get('lost', False))
    
    if not valid_rtts:
        return {
            "latency_min_ms": 0.0,
            "latency_max_ms": 0.0, 
            "latency_avg_ms": 0.0,
            "jitter_min_ms": 0.0,
            "jitter_max_ms": 0.0,
            "jitter_avg_ms": 0.0,
            "sent": len(records),
            "received": 0,
            "lost": lost_count
        }
    
    # Calculate jitter
    jitters = []
    if len(valid_rtts) > 1:
        jitters = [abs(valid_rtts[i] - valid_rtts[i-1]) for i in range(1, len(valid_rtts))]
    
    if not jitters:
        jitters = [0.0]  # If only one RTT, jitter is zero
    
    stats = {
        "latency_min_ms": min(valid_rtts),
        "latency_max_ms": max(valid_rtts),
        "latency_avg_ms": sum(valid_rtts) / len(valid_rtts),
        "jitter_min_ms": min(jitters),
        "jitter_max_ms": max(jitters),
        "jitter_avg_ms": sum(jitters) / len(jitters),
        "sent": len(records),
        "received": len(valid_rtts),
        "lost": lost_count
    }
    
    print("stats: ", stats)
    return stats

def publish_minute_stats(minute, records):
    stats = compute_stats(records)
    if not stats:
        return
    
    time_str = minute.strftime("%Y-%m-%dT%H:%M:00Z")
    
    stats_msg = {
        "agent_id": AGENT_ID,
        "time": time_str,
        **stats
    }
    
    MQTT_CLIENT.publish(
        f"netstats/{AGENT_ID}/minute",
        json.dumps(stats_msg),
        qos=0,
        retain=False
    )
    print(f"Published stats for {time_str}: {stats_msg}")

async def tcp_probe():
    global seq, in_flight, current_window, pending_window
    backoff = 0.5
    
    while True:
        try:
            reader, writer = await asyncio.open_connection(TCP_HOST, TCP_PORT)
            backoff = 0.5
            print("TCP connected")
            break
        except Exception as e:
            print(f"Connection failed: {e}, retrying in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 5)

    async def send_loop():
        global seq
        interval = 1 / RATE
        next_send_time = time.monotonic()
        
        while True:
            # Send packet
            t_send_ns = time.monotonic_ns()
            packet = {"agent_id": AGENT_ID, "seq": seq, "t_send_ns": t_send_ns}
            writer.write((json.dumps(packet) + "\n").encode())
            await writer.drain()
            
            in_flight[seq] = t_send_ns
            seq = (seq + 1) % 65536
            
            # Maintain precise timing
            next_send_time += interval
            current_time = time.monotonic()
            sleep_time = next_send_time - current_time
            
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                next_send_time = current_time

    async def recv_loop():
        global current_window, pending_window
        
        while True:
            line = await reader.readline()
            if not line:
                break
            
            try:
                data = json.loads(line)
                s = data["seq"]
                t_send = in_flight.pop(s, None)
                
                if t_send is None:
                    continue  # if late packet
                
                rtt_ms = (time.monotonic_ns() - t_send) / 1e6
                print(f"Received echo: seq={s}, RTT={rtt_ms:.2f} ms")
                
                now = datetime.now(timezone.utc)
                current_minute = now.replace(second=0, microsecond=0)
                
                # Handle window transitions and grace period
                if current_window["minute"] is None:
                    # First packet
                    current_window["minute"] = current_minute
                    current_window["data"] = []
                
                elif current_minute > current_window["minute"]:
                    # New minute started - move current to pending
                    if current_window["data"]:
                        pending_window = {
                            "minute": current_window["minute"],
                            "data": current_window["data"].copy()
                        }
                    
                    # Start new current window
                    current_window = {
                        "minute": current_minute,
                        "data": []
                    }
                
                # Add packet to appropriate window
                if current_window["minute"] == current_minute:
                    # Packet belongs to current minute
                    current_window["data"].append({"rtt": rtt_ms})
                    
                elif (pending_window["minute"] and 
                      pending_window["minute"] == current_minute - timedelta(minutes=1) and
                      now <= pending_window["minute"] + timedelta(seconds=60 + TIMEOUT_S)):
                    # Late packet within grace period for previous minute
                    pending_window["data"].append({"rtt": rtt_ms})
                    print(f"Late packet added to minute {pending_window['minute']}")
                else:
                    print(f"Packet too late, discarded: current_minute={current_minute}")
                
            except Exception as e:
                print(f"Error processing packet: {e}")

    async def timeout_sweep():
        global current_window, pending_window
        
        while True:
            now_ns = time.monotonic_ns()
            now_dt = datetime.now(timezone.utc)
            
            # Handle timeouts
            lost_seqs = [s for s, t_send in in_flight.items() 
                        if now_ns - t_send > TIMEOUT_S * 1e9]
            
            for s in lost_seqs:
                in_flight.pop(s)
                # Add lost packet to current window
                if current_window["minute"]:
                    current_window["data"].append({"rtt": 0.0, "lost": True})
            
            # Check if pending window's grace period has expired
            if (pending_window["minute"] and 
                now_dt > pending_window["minute"] + timedelta(seconds=60 + TIMEOUT_S)):
                
                publish_minute_stats(pending_window["minute"], pending_window["data"])
                pending_window = {"data": [], "minute": None}
            
            await asyncio.sleep(0.1)

    await asyncio.gather(send_loop(), recv_loop(), timeout_sweep())

if __name__ == "__main__":
    asyncio.run(tcp_probe())