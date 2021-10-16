PROJECT_NAME=gomigrator
DOCKER_COMPOSE=docker compose -f ./.docker/docker-compose.yaml --project-name="$(PROJECT_NAME)_dev"
DOCKER_COMPOSE_RUN=$(DOCKER_COMPOSE) run --rm 
DOCKER_COMPOSE_RUN_UNIT=$(DOCKER_COMPOSE_RUN) unit

ifndef ARGS
	ARGS=./...
endif

# This is a tweak of the following suggestions:
# https://gist.github.com/prwhite/8168133#gistcomment-1420062
help: ## This help dialog.
	@IFS=$$'\n' ; \
	help_lines=(`fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##/:/'`); \
	printf "%-30s %s\n" "target" "help" ; \
	printf "%-30s %s\n" "------" "----" ; \
	for help_line in $${help_lines[@]}; do \
			IFS=$$':' ; \
			help_split=($$help_line) ; \
			help_command=`echo $${help_split[0]} | sed -e 's/^ *//' -e 's/ *$$//'` ; \
			help_info=`echo $${help_split[2]} | sed -e 's/^ *//' -e 's/ *$$//'` ; \
			printf '\033[36m'; \
			printf "%-30s %s" $$help_command ; \
			printf '\033[0m'; \
			printf "%s\n" $$help_info; \
	done

.PHONY: down
down:
	$(DOCKER_COMPOSE) down --remove-orphans

.PHONY: tidy
tidy: ## Tidy the dependencies for the package
	$(DOCKER_COMPOSE_RUN_UNIT) go mod tidy

.PHONY: runtime
runtime: ## Start a golang runtime in docker with this code mounted
	$(DOCKER_COMPOSE_RUN_UNIT) bash

.PHONY: lint
lint: ## Run linter
	$(DOCKER_COMPOSE_RUN_UNIT) golangci-lint run ./...

.PHONY: fmt
fmt: ## Run go fmt
	$(DOCKER_COMPOSE_RUN_UNIT) go fmt ./...

.PHONY: unit-test
unit-test: ## Run the unit tests
	@$(DOCKER_COMPOSE_RUN_UNIT) go test -cover $(ARGS)
	@$(DOCKER_COMPOSE_RUN_UNIT) go tool cover -o=./coverage.html -html=./coverage.out

integration-test: ## Run the integration tests only
	@$(DOCKER_COMPOSE) up -d postgres
	@$(DOCKER_COMPOSE_RUN) postgres-integration go test -coverprofile ./coverage.out -cover -run ".Integration" $(ARGS)

.PHONY: test
test: unit-test integration-test ## Run unit and integration
