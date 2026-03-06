#!/bin/bash
cd /data/ai-stock-agent
git pull
source .venv/bin/activate

pids=$(pgrep -f "python web_app.py" || true)
if [ -n "$pids" ]; then
  for pid in $pids; do
    kill "$pid" 2>/dev/null && echo "Killed pid=$pid" || echo "pid=$pid already exited, skipping"
  done
  # 等待旧进程完全退出，最多等10秒
  for i in $(seq 1 20); do
    remaining=$(pgrep -f "python web_app.py" || true)
    [ -z "$remaining" ] && break
    sleep 0.5
  done
  if [ -n "$remaining" ]; then
    echo "Force killing remaining processes..."
    kill -9 $remaining 2>/dev/null || true
    sleep 1
  fi
else
  echo "No running web_app.py process found, skipping kill"
fi

nohup python3.11 web_app.py > app.log 2>&1 &
echo "Started, pid=$!, log=app.log"
