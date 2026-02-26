#!/bin/bash
clear
rm -rf ./var
rm -rf ./c2kv-log
source /app/scripts/env.sh
cd /app/src && /usr/local/go/bin/go build -o ../c2kv ./cmd/c2kv
cd /app/src && /usr/local/go/bin/go build -o ../c2kvctl ./cmd/c2kvctl
cd /app
/app/c2kv -m debug -c /app/config_dev.yaml
