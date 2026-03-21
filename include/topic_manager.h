#pragma once

#include "topic.h"

#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// TopicManager owns all topics and persists their metadata across restarts.
//
// On-disk layout:
//   <data_dir>/topics/<name>/partition-N/  — partition logs
//   <data_dir>/meta/topics.txt             — registry (one line: "name num_partitions\n")
class TopicManager
{
  public:
    explicit TopicManager(const std::filesystem::path &data_dir, uint64_t max_seg_bytes = kDefaultMaxSegmentBytes);

    // Returns err::kOk on success, err::kTopicAlreadyExists if the topic exists.
    int16_t CreateTopic(std::string_view name, int num_partitions);

    // Returns nullptr if not found.
    Topic *FindTopic(std::string_view name);
    const Topic *FindTopic(std::string_view name) const;

    // Returns sorted (name, num_partitions) pairs.
    std::vector<std::pair<std::string, int>> ListTopics() const;

  private:
    void LoadMetadata();
    void SaveMetadata() const;

    std::filesystem::path data_dir_;
    uint64_t max_seg_bytes_;

    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, std::unique_ptr<Topic>> topics_;
    std::unordered_map<std::string, int> meta_; // name → num_partitions
};
