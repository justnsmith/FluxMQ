# ─── FluxMQ Makefile ─────────────────────────────────────────────────────────
# Thin wrapper around CMake and the scripts/ directory.
# All C++ builds go through CMake; Go builds use `go build` directly.

BUILD    := build
SDK_DIR  := sdk
JOBS     := $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)

# ── CMake configure + build ─────────────────────────────────────────────────

.PHONY: configure
configure:
	cmake -S . -B $(BUILD) -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

$(BUILD)/src/fluxmq: configure
	cmake --build $(BUILD) --target fluxmq -j $(JOBS)

# Keep the old path working — symlink for backward compat with CI / Docker.
$(BUILD)/fluxmq: $(BUILD)/src/fluxmq
	ln -sf src/fluxmq $(BUILD)/fluxmq

# ── Top-level targets ───────────────────────────────────────────────────────

.PHONY: all
all: $(BUILD)/fluxmq test-binaries sdk bench

.PHONY: test-binaries
test-binaries: configure
	cmake --build $(BUILD) -j $(JOBS)

.PHONY: run
run: $(BUILD)/fluxmq
	./$(BUILD)/fluxmq

.PHONY: test
test: test-binaries
	./$(BUILD)/tests/fluxmq_test_log
	./$(BUILD)/tests/fluxmq_test_server
	./$(BUILD)/tests/fluxmq_test_broker
	./$(BUILD)/tests/fluxmq_test_chaos

# ── Go SDK ──────────────────────────────────────────────────────────────────

CLI   := $(BUILD)/fluxmq-cli
BENCH := $(BUILD)/fluxmq-bench

$(CLI): $(BUILD)/fluxmq
	cd $(SDK_DIR) && go build -o ../$(CLI) ./cmd/fluxmq

$(BENCH):
	@mkdir -p $(BUILD)
	cd $(SDK_DIR) && go build -o ../$(BENCH) ./cmd/fluxmq-bench

.PHONY: sdk
sdk: $(CLI)

.PHONY: bench
bench: $(BENCH)

.PHONY: sdk-test
sdk-test: $(BUILD)/fluxmq
	cd $(SDK_DIR) && go test ./tests/ -timeout 120s

.PHONY: sdk-vet
sdk-vet:
	cd $(SDK_DIR) && go vet ./...

# ── Docker ──────────────────────────────────────────────────────────────────

DOCKER_IMAGE := fluxmq-broker

.PHONY: docker-build docker-up docker-down docker-logs
docker-build:
	docker build -t $(DOCKER_IMAGE) .

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v

docker-logs:
	docker compose logs -f

# ── Housekeeping ────────────────────────────────────────────────────────────

.PHONY: clean
clean:
	rm -rf $(BUILD)

.PHONY: lint
lint:
	./scripts/lint.sh

.PHONY: format
format:
	./scripts/format.sh

.PHONY: format-check
format-check:
	./scripts/format.sh --check
