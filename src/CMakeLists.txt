# ─── FluxMQ Core Library ─────────────────────────────────────────────────────

add_library(fluxmq_core STATIC
    # Storage engine
    segment.cpp
    log.cpp

    # Network layer
    buffer.cpp
    reactor.cpp
    connection.cpp
    server.cpp

    # Broker core
    partition.cpp
    topic.cpp
    topic_manager.cpp
    group_coordinator.cpp
    handler.cpp

    # Cluster coordination
    cluster_store.cpp
    replica_client.cpp
    replication_manager.cpp
    leader_elector.cpp

    # Observability
    metrics.cpp
    metrics_server.cpp
)

target_include_directories(fluxmq_core PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(fluxmq_core PUBLIC pthread)

# Canonical alias
add_library(fluxmq::core ALIAS fluxmq_core)

# ─── Broker Executable ──────────────────────────────────────────────────────

add_executable(fluxmq main.cpp)
target_link_libraries(fluxmq PRIVATE fluxmq::core)

# Convenience target
add_custom_target(run
    COMMAND fluxmq
    DEPENDS fluxmq
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Starting FluxMQ broker"
)
