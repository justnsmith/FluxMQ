CXX      := g++
CXXFLAGS := -std=c++20 -Wall -Wextra -Iinclude -MMD -MP
BUILD    := build

TARGET          := $(BUILD)/fluxmq
TEST_LOG        := $(BUILD)/test_log
TEST_SERVER     := $(BUILD)/test_server
TEST_BROKER     := $(BUILD)/test_broker

# ── Source groups ─────────────────────────────────────────────────────────────

# Phase 1: storage engine
STORAGE_SRCS := src/segment.cpp src/log.cpp

# Phase 2: network layer
NET_SRCS := src/buffer.cpp src/reactor.cpp src/connection.cpp src/server.cpp

# Phase 3: broker core
# Phase 6: replication
BROKER_SRCS := src/partition.cpp src/topic.cpp src/topic_manager.cpp \
               src/group_coordinator.cpp src/handler.cpp \
               src/cluster_store.cpp src/replica_client.cpp \
               src/replication_manager.cpp src/leader_elector.cpp

# All library objects (shared by main binary and tests)
LIB_SRCS := $(STORAGE_SRCS) $(NET_SRCS) $(BROKER_SRCS)
LIB_OBJS := $(patsubst src/%.cpp,$(BUILD)/%.o,$(LIB_SRCS))

MAIN_OBJS := $(LIB_OBJS) $(BUILD)/main.o

# ── Main binary ───────────────────────────────────────────────────────────────

$(TARGET): $(MAIN_OBJS) | $(BUILD)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lpthread

$(BUILD)/main.o: src/main.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD)/%.o: src/%.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# ── Test binaries ─────────────────────────────────────────────────────────────

$(TEST_LOG): $(LIB_OBJS) $(BUILD)/test_log.o | $(BUILD)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lpthread

$(BUILD)/test_log.o: tests/test_log.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(TEST_SERVER): $(LIB_OBJS) $(BUILD)/test_server.o | $(BUILD)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lpthread

$(BUILD)/test_server.o: tests/test_server.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(TEST_BROKER): $(LIB_OBJS) $(BUILD)/test_broker.o | $(BUILD)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lpthread

$(BUILD)/test_broker.o: tests/test_broker.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# ── Targets ───────────────────────────────────────────────────────────────────

$(BUILD):
	mkdir -p $(BUILD)

run: $(TARGET)
	./$(TARGET)

test: $(TEST_LOG) $(TEST_SERVER) $(TEST_BROKER)
	./$(TEST_LOG)
	./$(TEST_SERVER)
	./$(TEST_BROKER)

# ── Go SDK ────────────────────────────────────────────────────────────────────

CLI         := $(BUILD)/fluxmq-cli
SDK_DIR     := sdk

$(CLI): $(TARGET) | $(BUILD)
	cd $(SDK_DIR) && go build -o ../$(CLI) ./cmd/fluxmq

sdk: $(CLI)

sdk-test: $(TARGET)
	cd $(SDK_DIR) && go test ./tests/ -timeout 60s

sdk-vet:
	cd $(SDK_DIR) && go vet ./...

# ── Auto-generated header dependencies ───────────────────────────────────────

ALL_OBJS := $(MAIN_OBJS) $(BUILD)/test_log.o $(BUILD)/test_server.o $(BUILD)/test_broker.o
-include $(ALL_OBJS:.o=.d)

clean:
	rm -rf $(BUILD)

lint:
	./scripts/lint.sh

format:
	./scripts/format.sh

format-check:
	./scripts/format.sh --check

.PHONY: run test sdk sdk-test sdk-vet clean lint format format-check

