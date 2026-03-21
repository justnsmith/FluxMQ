CXX      := g++
CXXFLAGS := -std=c++20 -Wall -Wextra -Iinclude
BUILD    := build
TARGET   := $(BUILD)/fluxmq
TEST_TARGET := $(BUILD)/test_log

# Library objects (shared between the main binary and tests).
LIB_SRCS := src/segment.cpp src/log.cpp
LIB_OBJS := $(patsubst src/%.cpp,$(BUILD)/%.o,$(LIB_SRCS))

SRCS := $(LIB_SRCS) src/main.cpp
OBJS := $(patsubst src/%.cpp,$(BUILD)/%.o,$(SRCS))

# ── Main binary ──────────────────────────────────────────────────────────────

$(TARGET): $(OBJS) | $(BUILD)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(BUILD)/%.o: src/%.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# ── Tests ────────────────────────────────────────────────────────────────────

$(TEST_TARGET): $(LIB_OBJS) $(BUILD)/test_log.o | $(BUILD)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(BUILD)/test_log.o: tests/test_log.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# ── Targets ──────────────────────────────────────────────────────────────────

$(BUILD):
	mkdir -p $(BUILD)

run: $(TARGET)
	./$(TARGET)

test: $(TEST_TARGET)
	./$(TEST_TARGET)

clean:
	rm -rf $(BUILD)

lint:
	./scripts/lint.sh

format:
	./scripts/format.sh

format-check:
	./scripts/format.sh --check

.PHONY: run test clean lint format format-check
