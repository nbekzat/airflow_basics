#!/bin/bash

# Configuration
export AIRFLOW_HOME="/Users/bekzatnurlanbekova/airflow"
LOG_DIR="$AIRFLOW_HOME/service-logs"
PID_DIR="$AIRFLOW_HOME/service-pids"
API_PORT=8080

# Ensure directories exist
mkdir -p "$LOG_DIR" "$PID_DIR"

# Core Airflow 3.3 components
COMPONENTS=("api-server" "scheduler" "dag-processor" "triggerer")

get_pid() {
    local component=$1
    if [ -f "$PID_DIR/$component.pid" ]; then
        cat "$PID_DIR/$component.pid"
    fi
}

is_running() {
    local pid=$1
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        return 0
    fi
    return 1
}

start_components() {
    echo "=== Starting Airflow 3.3 Components ==="
    
    # 1. Run migrations first
    echo "Running database migrations..."
    airflow db migrate > "$LOG_DIR/db-migrate.log" 2>&1
    
    for comp in "${COMPONENTS[@]}"; do
        pid=$(get_pid "$comp")
        if is_running "$pid"; then
            echo "[-] $comp is already running (PID: $pid)"
            continue
        fi

        echo "[+] Starting $comp..."
        if [ "$comp" == "api-server" ]; then
            nohup airflow api-server --port "$API_PORT" > "$LOG_DIR/$comp.log" 2>&1 &
        else
            nohup airflow "$comp" > "$LOG_DIR/$comp.log" 2>&1 &
        fi
        
        echo $! > "$PID_DIR/$comp.pid"
        sleep 1 # Allow initialization window
    done
    echo "All components targeted. Access UI at http://localhost:$API_PORT"
}

stop_components() {
    echo "=== Stopping Airflow 3.3 Components ==="
    for comp in "${COMPONENTS[@]}"; do
        pid=$(get_pid "$comp")
        if is_running "$pid"; then
            echo "[-] Stopping $comp (PID: $pid)..."
            kill "$pid"
            
            # Wait loop for graceful exit
            for i in {1..5}; do
                if ! kill -0 "$pid" 2>/dev/null; then
                    break
                fi
                sleep 0.5
            done
            
            # Force kill if still lingering
            if kill -0 "$pid" 2>/dev/null; then
                echo "[!] Force killing $comp..."
                kill -9 "$pid"
            fi
            rm -f "$PID_DIR/$comp.pid"
        else
            echo "[ ] $comp is not running."
        fi
    done
}

status_components() {
    echo "=== Airflow 3.3 Component Status ==="
    printf "%-20s %-10s %-15s\n" "Component" "Status" "PID"
    printf "%-20s %-10s %-15s\n" "---------" "------" "---"
    for comp in "${COMPONENTS[@]}"; do
        pid=$(get_pid "$comp")
        if is_running "$pid"; then
            printf "%-20s \e[32m%-10s\e[0m %-15s\n" "$comp" "RUNNING" "$pid"
        else
            printf "%-20s \e[31m%-10s\e[0m %-15s\n" "$comp" "STOPPED" "-"
        fi
    done
}

case "$1" in
    start)
        start_components
        ;;
    stop)
        stop_components
        ;;
    status)
        status_components
        ;;
    restart)
        stop_components
        sleep 2
        start_components
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart}"
        exit 1
        ;;
esac