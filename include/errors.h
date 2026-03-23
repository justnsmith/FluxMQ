#pragma once

#include <cstdint>

// Broker error codes (int16_t on the wire, matching Kafka's numbering for recognition).
namespace err
{
inline constexpr int16_t kOk = 0;
inline constexpr int16_t kOffsetOutOfRange = 1;
inline constexpr int16_t kUnknownTopic = 3;
inline constexpr int16_t kIllegalGeneration = 22;
inline constexpr int16_t kUnknownMemberId = 25;
inline constexpr int16_t kRebalanceInProgress = 27;
inline constexpr int16_t kTopicAlreadyExists = 36;
inline constexpr int16_t kGroupNotFound = 16;
inline constexpr int16_t kInvalidPartition = 10;
inline constexpr int16_t kInvalidRequest = 42;
inline constexpr int16_t kNotLeader = 6;           // request routed to wrong broker
inline constexpr int16_t kFencedLeaderEpoch = 74;  // stale leader epoch
inline constexpr int16_t kBrokerNotAvailable = 8;  // no leader elected yet
inline constexpr int16_t kDuplicateSequence = 46;  // idempotent: duplicate sequence number
inline constexpr int16_t kOutOfOrderSequence = 47; // idempotent: sequence gap detected
inline constexpr int16_t kUnknownProducerId = 48;  // idempotent: unregistered producer ID
} // namespace err
