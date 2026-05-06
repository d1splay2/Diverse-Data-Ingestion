.SILENT:

init:
	docker compose -f docker-compose-init.yaml up --abort-on-container-exit
	docker compose down
	docker rm init

test-all:
	make -C tests rustfs-integration
