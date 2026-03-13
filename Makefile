CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -Iinclude
BUILD    := build
TARGET   := $(BUILD)/fluxmq

SRCS := $(wildcard src/*.cpp)
OBJS := $(patsubst src/%.cpp,$(BUILD)/%.o,$(SRCS))

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(BUILD)/%.o: src/%.cpp | $(BUILD)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD):
	mkdir -p $(BUILD)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf $(BUILD)

lint:
	./scripts/lint.sh

format:
	./scripts/format.sh

format-check:
	./scripts/format.sh --check

.PHONY: run clean lint format format-check
