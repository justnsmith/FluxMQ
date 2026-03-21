CXX      := g++
CXXFLAGS := -std=c++20 -Wall -Wextra -Iinclude
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
BROKER_SRCS := src/partition.cpp src/topic.cpp src/topic_manager.cpp \
               src/group_coordinator.cpp src/handler.cpp

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

clean:
	rm -rf $(BUILD)

lint:
	./scripts/lint.sh

format:
	./scripts/format.sh

format-check:
	./scripts/format.sh --check

.PHONY: run test clean lint format format-check

