.PHONY: rebuild clear_docker_env
rebuild:clear_docker_env
	chmod +x ./scripts/clear_docker.sh
	docker compose -f ./deploy/dev/compose_dev.yaml -p c2kv-dev up -d

clear_docker_env:
	./scripts/clear_docker.sh