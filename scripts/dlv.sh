#!/bin/bash
clear
cd /app/src && /usr/local/go/bin/go build -o ../c2kv && cd ..
/go/bin/dlv --listen=:40000 --headless=true --api-version=2 --accept-multiclient exec ./c2kv