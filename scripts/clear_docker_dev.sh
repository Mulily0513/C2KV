#!/bin/bash
if [ -n "$(docker ps -aq)" ]; then
    docker rm -f $(docker ps -aq)
fi
if [ -n "$(docker images -q)" ]; then
    docker rmi -f $(docker images -q)
fi
if docker network ls | grep -q dev_mynetwork; then
    docker network rm dev_mynetwork
    echo "delete dev_mynetwork success。"
fi