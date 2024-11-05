#include "storage/index/b_plus_tree.h"
#include "storage/index/b_plus_tree_debug.h"
using std::cout, std::endl;
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
  ctx.AddIntoReadSet(std::move(guard));
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
    int id = KeyIndex(page, key);
    cur = reinterpret_cast<const InternalPage*>(page)->ValueAt(id);
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
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  // 没有节点
  if (root_page->root_page_id_ == INVALID_PAGE_ID) {
    auto new_page_id = bpm_->NewPage();
    guard = bpm_->WritePage(new_page_id);
    auto leaf_page = guard.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    root_page->root_page_id_ = new_page_id;
    leaf_page->Insert(0, key, value);
    return true;
  }
  // 找到对应叶子
  auto cur = root_page->root_page_id_;
  ctx.AddIntoWriteSet(std::move(guard));
  guard = bpm_->WritePage(cur);
  auto page = guard.AsMut<BPlusTreePage>();
  ctx.AddIntoWriteSet(std::move(guard));
  while (!page->IsLeafPage()) {
    int id = KeyIndex(page, key);
    cur = reinterpret_cast<const InternalPage*>(page)->ValueAt(id);
    guard = bpm_->WritePage(cur);
    page = guard.AsMut<BPlusTreePage>();
    ctx.AddIntoWriteSet(std::move(guard));
  }
  guard = std::move(ctx.write_set_.back());
  page = guard.AsMut<BPlusTreePage>();
  ctx.write_set_.pop_back();
  auto leaf_page = reinterpret_cast<LeafPage*>(page);
  int id = KeyIndex(page, key);
  if (id < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(id), key) == 0) {
    return false;
  }
  leaf_page->Insert(id, key, value);
  // 分裂
  while (page->GetSize() > page->GetMaxSize()) {
    if (page->IsLeafPage()) {
      auto right_id = bpm_->NewPage();
      auto right_guard = bpm_->WritePage(right_id);
      auto right_page = right_guard.AsMut<LeafPage>();
      right_page->Init(leaf_max_size_);
      right_page->SetNextPageId(leaf_page->GetNextPageId());
      leaf_page->SetNextPageId(right_id);
      right_page->SetKVs(leaf_page->GetKeys() + leaf_page->GetMinSize(), leaf_page->GetValues() + leaf_page->GetMinSize(), leaf_page->GetSize() - leaf_page->GetMinSize());
      right_page->SetParentPageId(leaf_page->GetParentPageId());
      leaf_page->SetSize(leaf_page->GetMinSize());
      // 如果是根节点
      if (root_page->root_page_id_ == guard.GetPageId()) {
        auto pre_id = bpm_->NewPage();
        auto pre_guard = bpm_->WritePage(pre_id);
        auto pre_page = pre_guard.AsMut<InternalPage>();
        pre_page->Init(internal_max_size_);
        pre_page->InsertValue(0, cur);
        pre_page->InsertValue(1, right_id);
        pre_page->InsertKey(1, right_page->KeyAt(0));
        leaf_page->SetParentPageId(pre_id);
        right_page->SetParentPageId(pre_id);
        pre_page->SetParentPageId(INVALID_PAGE_ID);
        root_page->root_page_id_ = pre_id;
        return true;
      }
      guard = std::move(ctx.write_set_.back());
      page = guard.AsMut<BPlusTreePage>();
      cur = guard.GetPageId();
      ctx.write_set_.pop_back();
      auto parent_page = reinterpret_cast<InternalPage*>(page);
      int id = KeyIndex(page, right_page->KeyAt(0));
      parent_page->InsertValue(id + 1, right_id);
      parent_page->InsertKey(id + 1, right_page->KeyAt(0));
    } else {
      auto internal_page = reinterpret_cast<InternalPage*>(page);
      auto right_id = bpm_->NewPage();
      auto right_guard = bpm_->WritePage(right_id);
      auto right_page = right_guard.AsMut<InternalPage>();
      auto mid_key = internal_page->KeyAt(internal_page->GetMinSize());
      right_page->Init(internal_max_size_);
      right_page->SetKeys(internal_page->GetKeys() + internal_page->GetMinSize(), internal_page->GetSize() - internal_page->GetMinSize());
      right_page->SetValues(internal_page->GetValues() + internal_page->GetMinSize(), internal_page->GetSize() - internal_page->GetMinSize());
      right_page->SetSize(internal_page->GetSize() - internal_page->GetMinSize());
      right_page->SetParentPageId(internal_page->GetParentPageId());
      internal_page->SetSize(internal_page->GetMinSize());
      if (root_page->root_page_id_ == guard.GetPageId()) {
        auto pre_id = bpm_->NewPage();
        auto pre_guard = bpm_->WritePage(pre_id);
        auto pre_page = pre_guard.AsMut<InternalPage>();
        pre_page->Init(internal_max_size_);
        pre_page->InsertValue(0, cur);
        pre_page->InsertValue(1, right_id);
        pre_page->InsertKey(1, mid_key);
        internal_page->SetParentPageId(pre_id);
        right_page->SetParentPageId(pre_id);
        pre_page->SetParentPageId(INVALID_PAGE_ID);
        root_page->root_page_id_ = pre_id;
        return true;
      }
      guard = std::move(ctx.write_set_.back());
      page = guard.AsMut<BPlusTreePage>();
      cur = guard.GetPageId();
      ctx.write_set_.pop_back();
      auto parent_page = reinterpret_cast<InternalPage*>(page);
      int id = KeyIndex(page, mid_key);
      parent_page->InsertValue(id + 1, right_id);
      parent_page->InsertKey(id + 1, mid_key);
    }
  }
  return true;
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
  // 对于叶子节点，返回的位置是大于等于key的最小的位置
  if (page->IsLeafPage()) {
    auto leaf_page = reinterpret_cast<const LeafPage*>(page);
    for (int i = 0; i < leaf_page->GetSize(); i++) {
      if (comparator_(leaf_page->KeyAt(i), key) >= 0) {
        return i;
      }
    }
    return leaf_page->GetSize();
  }
  // 对于内部节点，返回的位置是小于等于key的最大的位置
  else {
    auto internal_page = reinterpret_cast<const InternalPage*>(page);
    for (int i = internal_page->GetSize() - 1; i > 0; i --) {
      if (comparator_(internal_page->KeyAt(i), key) <= 0) {
        return i;
      }
    }
    return 0;
  }
  // 理论上二分的效率更高
  // 但是通常使用的时候，maxSize都会设置的比较小
  // 所以直接遍历查找不容易错，说不定还会更快
  // int lk = 0, rk = page->GetSize() - 1;
  // if (page->IsLeafPage()) {
  //   auto leaf_page = reinterpret_cast<const LeafPage*>(page);
  //   if (comparator_(key, leaf_page->KeyAt(rk)) > 0) {
  //     return page->GetSize();
  //   }
  //   while (lk < rk) {
  //     int mid = (lk + rk) / 2;
  //     if (comparator_(leaf_page->KeyAt(mid), key) < 0) {
  //       lk = mid + 1;
  //     }
  //     else {
  //       rk = mid;
  //     }
  //   }
  //   return lk
  // }
  // else {
  //   auto internal_page = reinterpret_cast<const InternalPage*>(page);
  //   if comparator_(key, internal_page->KeyAt(0)) < 0 {
  //     return 0;
  //   }
  //   while (lk < rk) {
  //     int mid = (lk + rk + 1) / 2;
  //     if (comparator_(internal_page->KeyAt(mid), key) <= 0) {
  //       lk = mid;
  //     }
  //     else{
  //       rk = mid - 1;
  //     }
  //   }
  //   return lk + 1;
  // }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
