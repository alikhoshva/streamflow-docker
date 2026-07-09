.PHONY: help up down clean test logs

# Default target
help:
	@echo "Available commands:"
	@echo "  make up      - Start Docker services"
	@echo "  make down    - Stop Docker services and clean data directories"
	@echo "  make clean   - Purge all raw, curated, rejects, and checkpoints"
	@echo "  make test    - Run clean data targets, then run pytest suite"
	@echo "  make logs    - Live-stream logs from all services"

up:
	mkdir -p logs/airflow
	chmod -R 777 logs/airflow
	docker compose -f docker/compose.yml up -d

down:
	docker compose -f docker/compose.yml down
	python3 scripts/cleanup_data.py --yes

clean:
	python3 scripts/cleanup_data.py --yes

test: clean
	pytest tests/

logs:
	docker compose -f docker/compose.yml logs -f


