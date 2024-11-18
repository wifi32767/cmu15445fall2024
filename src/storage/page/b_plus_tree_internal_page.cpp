//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { key_array_[index] = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return page_id_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKey(int index, const KeyType &key) {
  BUSTUB_ENSURE(index >= 1 && index <= GetSize(), "BPlusTreeInternalPage::InsertKey: pos is out of range");
  for (int i = GetSize(); i > index; i--) {
    key_array_[i] = key_array_[i - 1];
  }
  key_array_[index] = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertValue(int index, const ValueType &value) {
  BUSTUB_ENSURE(index >= 0 && index <= GetSize(), "BPlusTreeInternalPage::InsertValue: pos is out of range");
  for (int i = GetSize(); i > index; i--) {
    page_id_array_[i] = page_id_array_[i - 1];
  }
  page_id_array_[index] = value;
  ChangeSizeBy(1);
  // BUSTUB_ENSURE(GetSize() < INTERNAL_PAGE_SLOT_CNT, "BPlusTreeInternalPage::InsertValue: size is out of range");
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveKey(int index) {
  BUSTUB_ENSURE(index >= 1 && index < GetSize(), "BPlusTreeInternalPage::RemoveKey: pos is out of range");
  for (int i = index; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveValue(int index) {
  BUSTUB_ENSURE(index >= 0 && index < GetSize(), "BPlusTreeInternalPage::RemoveValue: pos is out of range");
  for (int i = index; i < GetSize() - 1; i++) {
    page_id_array_[i] = page_id_array_[i + 1];
  }
  ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeys(const KeyType *keys, int keys_size) {
  BUSTUB_ENSURE(keys_size <= GetMaxSize(), "BPlusTreeInternalPage::SetKeys: size is out of range");
  for (int i = 1; i <= keys_size; i++) {
    key_array_[i] = keys[i];
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValues(const ValueType *values, int values_size) {
  BUSTUB_ENSURE(values_size <= GetMaxSize(), "BPlusTreeInternalPage::SetValues: size is out of range");
  for (int i = 0; i < values_size; i++) {
    page_id_array_[i] = values[i];
  }
  SetSize(values_size);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
