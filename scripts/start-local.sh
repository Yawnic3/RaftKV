#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
mkdir -p bin data
go build -o bin/raftkv ./cmd/raftkv
go build -o bin/raftkvctl ./cmd/raftkvctl
pids=""
cleanup() {
    trap - INT TERM EXIT
    for pid in $pids; do
        kill "$pid" 2>/dev/null || true
    done
    wait || true
}
trap cleanup INT TERM EXIT
for node in 1 2 3; do
    bin/raftkv -config "config/node${node}.json" &
    pids="$pids $!"
done
echo 'Cluster running on ports 8001-8003. Ctrl+C stops nodes; data is retained.'
wait
