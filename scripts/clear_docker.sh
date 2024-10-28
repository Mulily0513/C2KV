#!/bin/bash
if [ -n "$(docker ps -aq)" ]; then
    docker rm -f $(docker ps -aq)
fi
if [ -n "$(docker images -q)" ]; then
    docker rmi -f $(docker images -q)
fi