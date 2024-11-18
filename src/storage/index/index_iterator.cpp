/**
 * index_iterator.cpp
 */
#include <algorithm>
#include <cassert>

#include "storage/index/index_iterator.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page_id, int index)
    : page_id_(page_id), index_(index), bpm_(bpm) {
  if (page_id_ != INVALID_PAGE_ID) {
    auto guard = bpm_->ReadPage(page_id_);
    auto page = guard.As<LeafPage>();
    data_ = std::make_pair(page->KeyAt(index_), page->ValueAt(index_));
  } else {
    data_ = std::make_pair(KeyType(), ValueType());
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == -1; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> { return data_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto cur_guard = bpm_->ReadPage(page_id_);
  auto cur_page = cur_guard.As<LeafPage>();
  ReadPageGuard guard;
  index_++;
  if (index_ >= cur_page->GetSize()) {
    page_id_ = cur_page->GetNextPageId();
    index_ = 0;
    if (page_id_ != INVALID_PAGE_ID) {
      guard = std::move(cur_guard);
      cur_guard = bpm_->ReadPage(page_id_);
      cur_page = cur_guard.As<LeafPage>();
    }
  }
  if (page_id_ != INVALID_PAGE_ID) {
    data_ = std::make_pair(cur_page->KeyAt(index_), cur_page->ValueAt(index_));
  } else {
    index_ = -1;
    data_ = std::make_pair(KeyType(), ValueType());
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
