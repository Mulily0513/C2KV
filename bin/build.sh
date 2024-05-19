#!/bin/bash
clear
rm -rf ./storagelog
rm -rf ./kvstorage
/usr/local/go/bin/go build -o c2kv
./c2kv