.PHONY: help up down clean test logs dashboard slides compile-deps sync-deps

# Default target
help:
	@echo "Available commands:"
	@echo "  make up           - Start Docker services"
	@echo "  make down         - Stop Docker services and clean data directories"
	@echo "  make clean        - Purge all raw, curated, rejects, and checkpoints"
	@echo "  make test         - Run clean data targets, then run pytest suite"
	@echo "  make logs         - Live-stream logs from all services"
	@echo "  make dashboard    - Launch Streamlit analytics dashboard"
	@echo "  make slides       - Compile the LaTeX slides presentation using Docker"
	@echo "  make compile-deps - Recompile requirements.txt from requirements.in using pip-compile"
	@echo "  make sync-deps    - Sync active Python environment with requirements.txt using pip-sync"


up:
	mkdir -p logs/airflow
	chmod 777 logs/airflow || true
	mkdir -p data
	chmod 777 data || true
	docker compose -f docker/compose.yml up -d
	@echo ""
	@echo "================================================================="
	@echo "  Streamflow Services Started Successfully!"
	@echo "================================================================="
	@echo "  Airflow Web UI:     http://localhost:8080 (admin / admin)"
	@echo "  Streamlit UI:       http://localhost:8501 (run 'make dashboard')"
	@echo "  Spark Job UI:       http://localhost:4040 (active during Spark jobs)"
	@echo "================================================================="

dashboard:
	streamlit run scripts/streamlit_dashboard.py

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

compile-deps:
	pip-compile requirements.in -o requirements.txt

sync-deps:
	pip-sync requirements.txt

