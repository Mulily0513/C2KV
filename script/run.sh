#!/bin/bash
/usr/sbin/sshd -D
source /app/env.sh
./c2kv -m debug -c ./config.yaml
