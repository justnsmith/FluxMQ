#include "topic_manager.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

TopicManager::TopicManager(const std::filesystem::path &data_dir, uint64_t max_seg_bytes)
    : data_dir_(data_dir), max_seg_bytes_(max_seg_bytes)
{
    std::filesystem::create_directories(data_dir_ / "topics");
    std::filesystem::create_directories(data_dir_ / "meta");
    LoadMetadata();
}

// ---------------------------------------------------------------------------

int16_t TopicManager::CreateTopic(std::string_view name, int num_partitions)
{
    std::unique_lock lock(mu_);
    std::string key(name);
    if (topics_.count(key))
        return 36; // err::kTopicAlreadyExists

    auto topic_dir = data_dir_ / "topics" / key;
    std::filesystem::create_directories(topic_dir);
    topics_[key] = std::make_unique<Topic>(key, num_partitions, topic_dir, max_seg_bytes_);
    meta_[key] = num_partitions;
    SaveMetadata();
    return 0; // err::kOk
}

Topic *TopicManager::FindTopic(std::string_view name)
{
    std::shared_lock lock(mu_);
    auto it = topics_.find(std::string(name));
    return it != topics_.end() ? it->second.get() : nullptr;
}

const Topic *TopicManager::FindTopic(std::string_view name) const
{
    std::shared_lock lock(mu_);
    auto it = topics_.find(std::string(name));
    return it != topics_.end() ? it->second.get() : nullptr;
}

std::vector<std::pair<std::string, int>> TopicManager::ListTopics() const
{
    std::shared_lock lock(mu_);
    std::vector<std::pair<std::string, int>> list;
    list.reserve(meta_.size());
    for (const auto &[name, np] : meta_)
        list.emplace_back(name, np);
    std::sort(list.begin(), list.end());
    return list;
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

void TopicManager::LoadMetadata()
{
    auto path = data_dir_ / "meta" / "topics.txt";
    if (!std::filesystem::exists(path))
        return;

    std::ifstream f(path);
    std::string name;
    int np;
    while (f >> name >> np) {
        meta_[name] = np;
        auto topic_dir = data_dir_ / "topics" / name;
        topics_[name] = std::make_unique<Topic>(name, np, topic_dir, max_seg_bytes_);
    }
}

void TopicManager::SaveMetadata() const
{
    auto tmp = data_dir_ / "meta" / "topics.txt.tmp";
    auto dst = data_dir_ / "meta" / "topics.txt";
    {
        std::ofstream f(tmp);
        for (const auto &[name, np] : meta_)
            f << name << " " << np << "\n";
    }
    std::filesystem::rename(tmp, dst);
}
