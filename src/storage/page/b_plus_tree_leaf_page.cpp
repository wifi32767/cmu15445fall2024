//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return rid_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(int index, const KeyType &key, const ValueType &value) {
  BUSTUB_ENSURE(index >= 0 && index <= GetSize(), "BPlusTreeLeafPage::Insert: pos is out of range");
  // 移动插入位置之后的元素
  for (int i = GetSize(); i > index; i--) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
  }
  // BUSTUB_ENSURE(index < (int)LEAF_PAGE_SLOT_CNT, "BPlusTreeLeafPage::Insert: index is out of range");
  key_array_[index] = key;
  rid_array_[index] = value;
  ChangeSizeBy(1);
  // BUSTUB_ENSURE(GetSize() <= (int)LEAF_PAGE_SLOT_CNT, "BPlusTreeLeafPage::Insert: size is out of range");
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(int index, const KeyType *keys, const ValueType *values, int size) {
  BUSTUB_ENSURE(index >= 0 && index <= GetSize(), "BPlusTreeLeafPage::Insert: pos is out of range");
  // BUSTUB_ENSURE((size_t)(size + GetSize()) <= LEAF_PAGE_SLOT_CNT, "BPlusTreeLeafPage::Insert: size is out of range");
  for (int i = 0; i < size; i++) {
    key_array_[GetSize() + i] = key_array_[index + i];
    rid_array_[GetSize() + i] = rid_array_[index + i];
    key_array_[index + i] = keys[i];
    rid_array_[index + i] = values[i];
  }
  ChangeSizeBy(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(int index) {
  BUSTUB_ENSURE(index >= 0 && index < GetSize(), "BPlusTreeLeafPage::Remove: pos is out of range");
  for (int i = index; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveFromLeft(int size) {
  BUSTUB_ASSERT(GetSize() >= size, "BPlusTreeLeafPage::RemoveFromLeft: size is out of range");
  for (int i = 0; i < GetSize() - size; i++) {
    key_array_[i] = key_array_[i + size];
    rid_array_[i] = rid_array_[i + size];
  }
  ChangeSizeBy(-size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveFromRight(int size) {
  BUSTUB_ASSERT(GetSize() >= size, "BPlusTreeLeafPage::RemoveFromRight: size is out of range");
  ChangeSizeBy(-size);
  // pass
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKVs(const KeyType *keys, const ValueType *values, int size) {
  for (int i = 0; i < size; i++) {
    key_array_[i] = keys[i];
    rid_array_[i] = values[i];
  }
  SetSize(size);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
