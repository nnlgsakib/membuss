# Membuss Makefile
#
# Targets:
#   make build          compile the daemon and CLI into bin/
#   make proto          regenerate protobuf Go bindings into proto/
#   make test           run `go test ./... -race -count=1`
#   make lint           run golangci-lint (skipped if not installed)
#   make run-daemon     run the daemon with ./membuss.yaml
#   make tidy           go mod tidy
#   make clean          remove bin/ and generated proto outputs
#
#   make docker-build   build the container image (tag: membuss:local)
#   make docker-run     run a one-off container with the named volume
#   make docker-stop    stop and remove the one-off container
#   make docker-logs    tail the container log
#   make docker-push    push the local image to a configurable registry
#   make docker-compose-up     docker compose up -d
#   make docker-compose-down   docker compose down -v
#   make docker-compose-logs   docker compose logs -f

GO            ?= go
PKG           := ./...
BUILD_DIR     := bin
DAEMON_BIN    := $(BUILD_DIR)/membuss
CLI_BIN       := $(BUILD_DIR)/membuss-cli
CONFIG_FILE   ?= membuss.yaml

# Docker knobs. Override on the command line, e.g.
#   make docker-push IMAGE=ghcr.io/me/membuss:0.1.0
DOCKER        ?= docker
IMAGE         ?= membuss:local
CONTAINER     ?= membuss
REGISTRY      ?=
COMPOSE       ?= docker compose

.PHONY: build proto test lint run-daemon tidy clean \
        docker-build docker-run docker-stop docker-logs docker-push \
        docker-compose-up docker-compose-down docker-compose-logs

build:
	mkdir -p $(BUILD_DIR)
	$(GO) build -o $(DAEMON_BIN)  ./cmd/membuss
	$(GO) build -o $(CLI_BIN)     ./cmd/membuss-cli

proto:
ifeq ($(OS),Windows_NT)
	powershell -ExecutionPolicy Bypass -File scripts/gen-proto.ps1
else
	bash scripts/gen-proto.sh
endif

test:
	$(GO) test $(PKG) -race -count=1

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run $(PKG); \
	else \
		echo "golangci-lint not installed; skipping"; \
	fi

run-daemon: build
	$(DAEMON_BIN) -config $(CONFIG_FILE)

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BUILD_DIR) proto/*.pb.go

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

# docker-build produces a single image tagged $(IMAGE) using the
# multi-stage Dockerfile in the repo root. The build-arg BUILDDATE
# is propagated to the image label so CI builds can be traced.
docker-build:
	$(DOCKER) build -t $(IMAGE) --build-arg BUILDDATE="$$(date -u +%Y-%m-%dT%H:%M:%SZ)" .

# docker-run brings up a one-off container with the same port and
# volume layout that docker-compose.yml describes, but without
# the compose tool in the loop. Handy for local debugging.
docker-run: docker-build
	$(DOCKER) run -d --name $(CONTAINER) \
		-p 4001:4001/tcp \
		-p 4001:4001/udp \
		-p 5001:5001/tcp \
		-p 8080:8080/tcp \
		-p 50051:50051/tcp \
		-v membuss-data:/var/lib/membuss \
		$(IMAGE)

docker-stop:
	-$(DOCKER) rm -f $(CONTAINER)

docker-logs:
	$(DOCKER) logs -f $(CONTAINER)

# docker-push tags the local image for the configured registry
# and pushes it. Override REGISTRY (or set IMAGE explicitly) to
# target a non-default registry.
docker-push: docker-build
	@test -n "$(REGISTRY)" || (echo "REGISTRY is empty; set REGISTRY=ghcr.io/me or pass IMAGE=..."; exit 1)
	$(DOCKER) tag $(IMAGE) $(REGISTRY)/$(IMAGE)
	$(DOCKER) push $(REGISTRY)/$(IMAGE)

docker-compose-up:
	$(COMPOSE) up -d --build

docker-compose-down:
	$(COMPOSE) down -v

docker-compose-logs:
	$(COMPOSE) logs -f
