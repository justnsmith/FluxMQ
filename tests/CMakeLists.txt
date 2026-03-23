# ─── FluxMQ Test Executables ─────────────────────────────────────────────────

# Storage engine tests (segment, log, partition, compaction)
add_executable(fluxmq_test_log test_log.cpp)
target_link_libraries(fluxmq_test_log PRIVATE fluxmq::core)

# TCP server tests (connection lifecycle, pipelining)
add_executable(fluxmq_test_server test_server.cpp)
target_link_libraries(fluxmq_test_server PRIVATE fluxmq::core)

# Broker protocol tests (produce, fetch, consumer groups, idempotent)
add_executable(fluxmq_test_broker test_broker.cpp)
target_link_libraries(fluxmq_test_broker PRIVATE fluxmq::core)

# Chaos tests (replication, failover, ISR)
add_executable(fluxmq_test_chaos test_chaos.cpp)
target_link_libraries(fluxmq_test_chaos PRIVATE fluxmq::core)

# ─── CTest registration ─────────────────────────────────────────────────────

enable_testing()
add_test(NAME log_tests    COMMAND fluxmq_test_log)
add_test(NAME server_tests COMMAND fluxmq_test_server)
add_test(NAME broker_tests COMMAND fluxmq_test_broker)
add_test(NAME chaos_tests  COMMAND fluxmq_test_chaos)
