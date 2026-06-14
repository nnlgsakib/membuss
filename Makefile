# Membuss — Phase 0 Makefile
#
# Targets:
#   make build        compile the daemon and CLI into bin/
#   make proto        regenerate protobuf Go bindings into proto/
#   make test         run `go test ./... -race -count=1`
#   make lint         run golangci-lint (skipped if not installed)
#   make run-daemon   run the daemon with ./membuss.yaml
#   make tidy         go mod tidy
#   make clean        remove bin/ and generated proto outputs

GO            ?= go
PKG           := ./...
BUILD_DIR     := bin
DAEMON_BIN    := $(BUILD_DIR)/membuss
CLI_BIN       := $(BUILD_DIR)/membuss-cli
CONFIG_FILE   ?= membuss.yaml

.PHONY: build proto test lint run-daemon tidy clean

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
