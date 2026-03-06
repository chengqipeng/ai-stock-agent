#!/bin/bash
cd /data/ai-stock-agent
git pull
source .venv/bin/activate

pids=$(pgrep -f "python web_app.py" || true)
if [ -n "$pids" ]; then
  echo "$pids" | while read pid; do
    kill "$pid" 2>/dev/null && echo "Killed pid=$pid" || echo "Failed to kill pid=$pid, may have already exited"
  done
else
  echo "No running web_app.py process found, skipping kill"
fi

nohup python3.11 web_app.py > app.log 2>&1 &
echo "Started, pid=$!, log=app.log"
