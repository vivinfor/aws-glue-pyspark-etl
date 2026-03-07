SHELL := /bin/bash

PORT ?= 8080
SERVICE ?= app

BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m

.PHONY: help
help:
	@echo "$(BLUE)=== Comandos Disponíveis ===$(NC)"
	@echo "$(GREEN)make up$(NC)              - Inicia os containers"
	@echo "$(GREEN)make up-build$(NC)        - Inicia e reconstrói os containers"
	@echo "$(GREEN)make build$(NC)           - Reconstrói a imagem sem cache"
	@echo "$(GREEN)make down$(NC)            - Para os containers"
	@echo "$(GREEN)make logs$(NC)            - Mostra logs em tempo real"
	@echo "$(GREEN)make shell$(NC)           - Acessa o shell do container"
	@echo "$(GREEN)make test$(NC)            - Executa os testes"
	@echo "$(GREEN)make pipeline$(NC)        - Executa o ETL pipeline"
	@echo "$(GREEN)make clean$(NC)           - Remove containers e volumes"
	@echo "$(GREEN)make kill-port$(NC)       - Mata processo rodando na porta definida"
	@echo ""
	@echo "$(YELLOW)Variáveis:$(NC)"
	@echo "  PORT=8080    - Porta do serviço (padrão: 8080)"
	@echo "  SERVICE=app  - Nome do serviço (padrão: app)"

.PHONY: kill-port
kill-port:
	@echo "$(YELLOW)Verificando processos na porta $(PORT)...$(NC)"
	@PID=$$(lsof -ti tcp:$(PORT)); \
	if [ -n "$$PID" ]; then \
		echo "$(RED)Matando processo $$PID na porta $(PORT)...$(NC)"; \
		kill -9 $$PID; \
		echo "$(GREEN)Processo finalizado.$(NC)"; \
	else \
		echo "$(GREEN)Nenhum processo rodando na porta $(PORT).$(NC)"; \
	fi

.PHONY: up
up:
	@echo "$(BLUE)Iniciando containers...$(NC)"
	docker-compose up

.PHONY: up-build
up-build:
	@echo "$(BLUE)Iniciando e reconstruindo containers...$(NC)"
	docker-compose up --build

.PHONY: build
build:
	@echo "$(BLUE)Reconstruindo imagem sem cache...$(NC)"
	docker-compose build --no-cache

.PHONY: down
down:
	@echo "$(BLUE)Parando containers...$(NC)"
	docker-compose down

.PHONY: logs
logs:
	@echo "$(BLUE)Mostrando logs (últimas 200 linhas)...$(NC)"
	docker-compose logs -f --tail=200

.PHONY: shell
shell:
	@echo "$(BLUE)Acessando shell do container $(SERVICE)...$(NC)"
	docker exec -it $(SERVICE) /bin/bash

.PHONY: test
test:
	@echo "$(BLUE)Executando testes...$(NC)"
	docker-compose exec $(SERVICE) pytest tests/

.PHONY: pipeline
pipeline:
	@echo "$(BLUE)Executando ETL pipeline...$(NC)"
	docker-compose exec $(SERVICE) python run_pipeline.py

.PHONY: clean
clean:
	@echo "$(YELLOW)Removendo containers e volumes...$(NC)"
	docker-compose down -v
	@echo "$(GREEN)Limpeza concluída!$(NC)"

.PHONY: status
status:
	@echo "$(BLUE)Status dos containers:$(NC)"
	docker-compose ps

.PHONY: restart
restart: down up
	@echo "$(GREEN)Containers reiniciados!$(NC)"

.PHONY: prune
prune:
	@echo "$(YELLOW)Removendo container $(SERVICE) se existir...$(NC)"
	@docker rm -f $(SERVICE) 2>/dev/null || true
	@echo "$(GREEN)Pronto!$(NC)"
