#!/bin/bash
clear
/usr/local/go/bin/go build -o c2kv
/go/bin/dlv --listen=:40000 --headless=true --api-version=2 --accept-multiclient exec ./c2kv