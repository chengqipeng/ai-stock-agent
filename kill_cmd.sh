#!/bin/bash
# cd /data/ai-stock-agent
# git pull
# source .venv/bin/activate

# 检测操作系统
OS_TYPE="$(uname -s)"

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

# 杀掉所有占用80端口的进程（兼容 macOS 和 Linux）
get_port_pids() {
  if [ "$OS_TYPE" = "Darwin" ]; then
    # macOS: 使用 lsof
    lsof -ti:80 2>/dev/null || true
  else
    # Linux: 优先使用 ss + awk，回退到 lsof
    if command -v ss &>/dev/null; then
      ss -tlnp sport = :80 2>/dev/null | grep -oP 'pid=\K[0-9]+' | sort -u || true
    elif command -v lsof &>/dev/null; then
      lsof -ti:80 2>/dev/null || true
    elif [ -d /proc ]; then
      # 最后手段：通过 /proc/net/tcp 查找（端口80 = 0050）
      fuser 80/tcp 2>/dev/null || true
    fi
  fi
}

port_pids=$(get_port_pids)
if [ -n "$port_pids" ]; then
  echo "Killing processes on port 80: $port_pids"
  echo "$port_pids" | xargs kill 2>/dev/null || true
  sleep 2
  # 如果还有残留，强制杀掉
  port_pids=$(get_port_pids)
  if [ -n "$port_pids" ]; then
    echo "Force killing remaining port 80 processes: $port_pids"
    echo "$port_pids" | xargs kill -9 2>/dev/null || true
    sleep 1
  fi
else
  echo "No process on port 80, skipping"
fi

echo "Killed, pid=$!, log=app.log"
