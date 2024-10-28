#!/bin/bash
/usr/sbin/sshd -D &
source /app/scripts/env.sh
/app/c2kv -m debug -c /app/config_dev.yaml
