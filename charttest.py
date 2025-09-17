#!/usr/bin/env python3
import sqlite3, json, time, os

DB_FILE = "netstats.db"
OUT_FILE = "chart.html"

TEMPLATE = """<!doctype html>
<html>
<head>
  <meta http-equiv="refresh" content="60">
  <meta charset="utf-8" />
  <title>NetStats Chart</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
  <h2>Latency & Jitter (from SQLite)</h2>
  <canvas id="chart" width="800" height="400"></canvas>
  <script>
    const data = {data_json};
    const labels = data.map(r => r.minute);
    const latency = data.map(r => r.latency);
    const jitter = data.map(r => r.jitter);

    new Chart(document.getElementById('chart'), {{
      type: 'line',
      data: {{
        labels: labels,
        datasets: [
          {{
            label: 'Latency (ms)',
            data: latency,
            borderColor: 'blue',
            fill: false
          }},
          {{
            label: 'Jitter (ms)',
            data: jitter,
            borderColor: 'orange',
            fill: false
          }}
        ]
      }},
      options: {{
        responsive: false,
        scales: {{
          x: {{ title: {{ display: true, text: 'Minute (UTC)' }} }},
          y: {{ title: {{ display: true, text: 'ms' }} }}
        }}
      }}
    }});
  </script>
</body>
</html>"""

def generate_html():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
    SELECT minute_utc, latency_avg_ms, jitter_avg_ms
    FROM (
        SELECT minute_utc, latency_avg_ms, jitter_avg_ms
        FROM minute_stats
        ORDER BY minute_utc DESC
        LIMIT 200
    )
    ORDER BY minute_utc ASC
    """)
    rows = [{"minute": r[0], "latency": r[1], "jitter": r[2]} for r in c.fetchall()]
    conn.close()

    html = TEMPLATE.format(data_json=json.dumps(rows))
    with open(OUT_FILE, "w") as f:
        f.write(html)
    print(f"[+] Wrote {OUT_FILE} at {time.strftime('%X')}")


if __name__ == "__main__":
    last_minute = None
    while True:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT MAX(minute_utc) FROM minute_stats")
        latest = c.fetchone()[0]
        conn.close()

        if latest != last_minute:
            last_minute = latest
            generate_html()
        time.sleep(2)   # check every 2s
