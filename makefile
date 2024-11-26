.PHONY: rebuild clear_docker_env build
build:
	docker compose -f ./deploy/dev/compose_dev.yaml -p c2kv-dev up -d

rebuild:clear_docker_env
	docker compose -f ./deploy/dev/compose_dev.yaml -p c2kv-dev up -d

clear_docker_env:
	chmod +x ./scripts/clear_docker_dev.sh
	./scripts/clear_docker_dev.sh