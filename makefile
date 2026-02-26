.PHONY: rebuild clear_docker_env dev_build build c2kv c2kvctl

build: c2kv c2kvctl

c2kv:
	go build -o ./bin/c2kv ./cmd/c2kv

c2kvctl:
	go build -o ./bin/c2kvctl ./cmd/c2kvctl

dev_build:
	docker compose -f ./deploy/dev/compose_dev.yaml -p c2kv-dev up -d --build

rebuild:clear_docker_env
	docker compose -f ./deploy/dev/compose_dev.yaml -p c2kv-dev up -d --build

clear_docker_env:
	chmod +x ./scripts/clear_docker_dev.sh
	./scripts/clear_docker_dev.sh
