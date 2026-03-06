#!/bin/bash
cd /data/ai-stock-agent
git pull
source .venv/bin/activate

pid=$(pgrep -f "python web_app.py")
[ -n "$pid" ] && kill "$pid" && echo "Killed pid=$pid"

nohup python3.11 web_app.py > app.log 2>&1 &
echo "Started, pid=$!, log=app.log"
