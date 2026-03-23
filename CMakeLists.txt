cmake_minimum_required(VERSION 3.20)
project(fluxmq LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build all components
add_subdirectory(src)
add_subdirectory(tests)
