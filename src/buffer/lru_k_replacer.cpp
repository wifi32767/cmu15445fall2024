//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  if (curr_size_ == 0) {
    return std::nullopt;
  }
  int val = 0;
  frame_id_t frame_id;
  auto last = std::numeric_limits<uint64_t>::max();
  {
    std::lock_guard<std::mutex> lock(latch_);
    for (auto &[k, v] : node_store_) {
      if (v.is_evictable_) {
        int curr_val;
        if (v.history_.size() < k_) {
          curr_val = std::numeric_limits<int>::max();
        } else {
          curr_val = current_timestamp_ - v.history_.front();
        }
        if (curr_val > val || (curr_val == val && v.history_.front() < last)) {
          val = curr_val;
          frame_id = k;
          last = v.history_.front();
        }
      }
    }
  }
  if (val == 0) {
    return std::nullopt;
  }
  Remove(frame_id);
  return frame_id;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT((frame_id >= 0) && (static_cast<size_t>(frame_id) < replacer_size_),
                "LRUKReplacer::RecordAccess: Invalid frame id");
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    if (curr_size_ >= replacer_size_) {
      return;
    }
    LRUKNode node;
    node.fid_ = frame_id;
    node.k_ = k_;
    node.is_evictable_ = false;
    node_store_[frame_id] = node;
  }
  auto &node = node_store_[frame_id];
  node.history_.push_back(current_timestamp_++);
  if (node.history_.size() > k_) {
    node.history_.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT((frame_id >= 0) && (static_cast<size_t>(frame_id) < replacer_size_),
                "LRUKReplacer::SetEvictable: Invalid frame id");
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto &node = node_store_[frame_id];
  if (node.is_evictable_ == set_evictable) {
    return;
  }
  node.is_evictable_ = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT((frame_id >= 0) && (static_cast<size_t>(frame_id) < replacer_size_),
                "LRUKReplacer::Remove: Invalid frame id");
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  if (node_store_[frame_id].is_evictable_) {
    curr_size_--;
    node_store_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
