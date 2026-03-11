.PHONY: help postgres-up postgres-down fetch-data dbt-run dbt-test export clean

help:
	@echo "IBGE Paises Analytics Pipeline"
	@echo ""
	@echo "Targets:"
	@echo "  make postgres-up       — Inicia PostgreSQL local"
	@echo "  make postgres-down     — Para PostgreSQL"
	@echo "  make fetch-data        — Baixa dados da API IBGE"
	@echo "  make dbt-run           — Executa transformações dbt"
	@echo "  make dbt-test          — Testes de qualidade"
	@echo "  make export            — Reservado (nao implementado neste repo)"
	@echo "  make clean             — Remove arquivos temporários"

postgres-up:
	docker-compose up -d
	@echo "✓ PostgreSQL disponível em localhost:5433"

postgres-down:
	docker-compose down

fetch-data:
	python src/data_loader.py

dbt-run:
	cd dbt && dbt seed --select country_continent_depara --full-refresh && dbt run

dbt-test:
	cd dbt && dbt test

export:
	@echo "Export para DuckDB nao esta implementado neste repositorio."

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf dbt/target/ dbt/logs/
