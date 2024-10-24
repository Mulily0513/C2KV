#!/script/bash
clear
rm -rf /app/data
rm -rf /app/c2kv-log
source /app/env.sh
cd /app/src && /usr/local/go/bin/go build -o ../c2kv
./c2kv