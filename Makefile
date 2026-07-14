.PHONY: help up down clean test logs slides

# Default target
help:
	@echo "Available commands:"
	@echo "  make up      - Start Docker services"
	@echo "  make down    - Stop Docker services and clean data directories"
	@echo "  make clean   - Purge all raw, curated, rejects, and checkpoints"
	@echo "  make test    - Run clean data targets, then run pytest suite"
	@echo "  make logs    - Live-stream logs from all services"
	@echo "  make slides  - Compile the LaTeX slides presentation using Docker"

up:
	mkdir -p logs/airflow
	chmod 777 logs/airflow || true
	mkdir -p data
	chmod 777 data || true
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

slides:
	docker run --platform linux/amd64 --rm -v $(CURDIR)/presentation:/workdir -w /workdir ghcr.io/xu-cheng/texlive-full latexmk -pdf -interaction=nonstopmode slides.tex


