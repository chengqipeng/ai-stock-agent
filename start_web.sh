#!/bin/bash
cd /data/ai-stock-agent
git pull
source .venv/bin/activate

pid=$(pgrep -f "python web_app.py" || true)
if [ -n "$pid" ]; then
  kill "$pid" && echo "Killed pid=$pid"
else
  echo "No running web_app.py process found, skipping kill"
fi

nohup python3.11 web_app.py > app.log 2>&1 &
echo "Started, pid=$!, log=app.log"
