#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance.
  Context ctx;
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  auto cur = root_page->root_page_id_;
  while (cur != INVALID_PAGE_ID){
    guard = bpm_->ReadPage(cur);
    auto page = guard.As<BPlusTreePage>();
    ctx.AddIntoReadSet(std::move(guard));
    if (page->IsLeafPage()) {
      int id = KeyIndex(page, key);
      if (id == -1) {
        return false;
      }
      result->push_back(reinterpret_cast<const LeafPage*>(page)->ValueAt(id));
      return true;
    }
    else {
      int id = KeyIndex(page, key);
      cur = reinterpret_cast<const InternalPage*>(page)->ValueAt(id);
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // Context ctx;
  // ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  // auto root_page = guard.As<BPlusTreeHeaderPage>();
  // if (root_page->root_page_id_ == INVALID_PAGE_ID) {
  //   return INDEXITERATOR_TYPE();
  // }
  // auto cur = root_page->root_page_id_;
  // while (cur != INVALID_PAGE_ID){
  //   guard = bpm_->ReadPage(cur);
  //   auto page = guard.As<BPlusTreePage>();
  //   ctx.AddIntoReadSet(std::move(guard));
  //   if (page->IsLeafPage()) {
  //     auto leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(page);
  //     auto key = leaf_page->KeyAt(0);
  //     auto value = leaf_page->ValueAt(0);
  //     return INDEXITERATOR_TYPE(key, value, comparator_, bpm_);
  //   }
  //   else {
  //     auto internal_page = reinterpret_cast<BPlusTreeInternalPage*>(page);
  //     cur = internal_page->ValueAt(0);
  //   }
  // }
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyIndex(const BPlusTreePage* page, const KeyType &key) -> int {
  int lk = 0, rk = page->GetSize() - 1;
  if (page->IsLeafPage()) {
    auto leaf_page = reinterpret_cast<const LeafPage*>(page);
    while (lk <= rk) {
      int mid = (lk + rk) / 2;
      if (comparator_(leaf_page->KeyAt(mid), key) < 0) {
        lk = mid + 1;
      } else if (comparator_(leaf_page->KeyAt(mid), key) > 0) {
        rk = mid - 1;
      } else {
        return mid;
      }
    }
    return -1;
  }
  else {
    auto internal_page = reinterpret_cast<const InternalPage*>(page);
    while (lk < rk) {
      int mid = (lk + rk + 1) / 2;
      if (comparator_(key, internal_page->KeyAt(mid)) < 0) {
        rk = mid - 1;
      } else {
        lk = mid;
      }
    }
    return lk;
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
