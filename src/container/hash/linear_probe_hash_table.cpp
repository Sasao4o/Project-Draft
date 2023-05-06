//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include <string>

#include <utility>

#include <vector>

#include <cmath>

#include "common/exception.h"

#include "common/logger.h"

#include "common/rid.h"

#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

  template < typename KeyType, typename ValueType, typename KeyComparator >
    HASH_TABLE_TYPE::LinearProbeHashTable(const std::string & name,
      BufferPoolManager * buffer_pool_manager,
      const KeyComparator & comparator,
        size_t num_buckets,
        HashFunction < KeyType > hash_fn): buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {

      Page * newPage = buffer_pool_manager_ -> NewPage( & header_page_id_);
      auto headerPage = reinterpret_cast < HashTableHeaderPage * > (newPage -> GetData());
      headerPage -> SetPageId(header_page_id_);

      headerPage -> SetSize(num_buckets);
      
      auto blockNum =ceil ((double)num_buckets / (double)BLOCK_ARRAY_SIZE);
      page_id_t tmpPageId;
      for (size_t i = 0; i < blockNum; i++) {

        buffer_pool_manager -> NewPage( & tmpPageId);

        headerPage -> AddBlockPageId(tmpPageId);

        buffer_pool_manager -> UnpinPage(tmpPageId, true);
      }

      buffer_pool_manager -> UnpinPage(header_page_id_, true);
    }
  template < typename KeyType, typename ValueType, typename KeyComparator >
    bool HASH_TABLE_TYPE::GetValue(Transaction * transaction,
      const KeyType & key, std::vector < ValueType > * result) {
      table_latch_.RLock();
      Page * hPage = buffer_pool_manager_ -> FetchPage(header_page_id_);
      HashTableHeaderPage * headerPage = reinterpret_cast < HashTableHeaderPage * > (hPage -> GetData());

      auto mmHash = hash_fn_.GetHash(key);

      auto targetBlockIndex = mmHash % headerPage -> NumBlocks();
      auto blockPageId = headerPage -> GetBlockPageId(targetBlockIndex);

      Page * page = buffer_pool_manager_ -> FetchPage(blockPageId);
      HASH_TABLE_BLOCK_TYPE * blockPage = reinterpret_cast < HASH_TABLE_BLOCK_TYPE * > (page -> GetData());
      auto res = false;
      bool foundEmptySlot = false;
      size_t visitedBlocks = 0;
      while (!foundEmptySlot && visitedBlocks < headerPage -> NumBlocks()) {
        for (long unsigned int i = 0; i < BLOCK_ARRAY_SIZE; i++) {
          if (blockPage -> IsValid(i) && comparator_(blockPage -> KeyAt(i), key) == 0) {
           result->push_back(blockPage->ValueAt(i));
           res = true;
           break;

          } else if (!blockPage -> IsOccupied(i)) {
            foundEmptySlot = true;
          }
        }
        targetBlockIndex = (targetBlockIndex + 1) % headerPage -> NumBlocks();
        buffer_pool_manager_ -> UnpinPage(blockPageId, true);
        blockPageId = headerPage -> GetBlockPageId(targetBlockIndex);
        page = buffer_pool_manager_ -> FetchPage(blockPageId);
        blockPage = reinterpret_cast < HASH_TABLE_BLOCK_TYPE * > (page -> GetData());
        visitedBlocks++;
      }
      //unpin all page
      buffer_pool_manager_ -> UnpinPage(blockPageId, true);
      buffer_pool_manager_ -> UnpinPage(header_page_id_, true);
      this->table_latch_.RUnlock();
      return res;
    }

  template < typename KeyType, typename ValueType, typename KeyComparator >
    bool HASH_TABLE_TYPE::Insert(Transaction * transaction,
      const KeyType & key,
        const ValueType & value) {
      table_latch_.RLock();
      Page * hPage = buffer_pool_manager_ -> FetchPage(header_page_id_);
      if (hPage == nullptr) return false;
      HashTableHeaderPage * headerPage = reinterpret_cast < HashTableHeaderPage * > (hPage -> GetData());

      auto mmHash = hash_fn_.GetHash(key);
 
    
      auto targetBlockIndex = mmHash % headerPage -> NumBlocks();
      
      auto blockPageId = headerPage -> GetBlockPageId(targetBlockIndex);
      Page * page = buffer_pool_manager_ -> FetchPage(blockPageId);
      HASH_TABLE_BLOCK_TYPE * blockPage = reinterpret_cast < HASH_TABLE_BLOCK_TYPE * > (page -> GetData());
      auto res = false;
      
      bool found = blockPage -> isKeyValueExist(key, value, comparator_);;
      testUse = found;
      size_t visitedBlocks = 1;
      //Check if the block is empty and no duplicate (key,value)
      while (!found && blockPage -> IsFull() && visitedBlocks <= headerPage -> NumBlocks()) {
        
          targetBlockIndex = (targetBlockIndex + 1) % headerPage -> NumBlocks();
          buffer_pool_manager_ -> UnpinPage(blockPageId, true);
          blockPageId = headerPage -> GetBlockPageId(targetBlockIndex);
          page = buffer_pool_manager_ -> FetchPage(blockPageId);
          blockPage = reinterpret_cast < HASH_TABLE_BLOCK_TYPE * > (page -> GetData());
          found = blockPage -> isKeyValueExist(key, value, comparator_);
          visitedBlocks++;
      }

      if (!found && visitedBlocks > headerPage -> NumBlocks()) {
        //Resizing
            Resize(headerPage->GetSize());
            return this->Insert(transaction, key, value);
      } else if (!found) {
         page->WLatch();
        for (long unsigned i = 0; i < BLOCK_ARRAY_SIZE; i++) {
          if (!blockPage -> IsOccupied(i)) {
            blockPage -> Insert(i, key, value);
            headerPage->count ++;
            res = true;
            break;
          }
        }
         page->WUnlatch();
      }
      //unpin all page
      buffer_pool_manager_ -> UnpinPage(blockPageId, true);
      buffer_pool_manager_ -> UnpinPage(header_page_id_, true);
      this->table_latch_.RUnlock();
      return res; 
    }


  template < typename KeyType, typename ValueType, typename KeyComparator >
    bool HASH_TABLE_TYPE::Remove(Transaction * transaction,
      const KeyType & key,
        const ValueType & value) {
       table_latch_.RLock();     
      Page * hPage = buffer_pool_manager_ -> FetchPage(header_page_id_);
      if (hPage == nullptr) return false;
      HashTableHeaderPage * headerPage = reinterpret_cast < HashTableHeaderPage * > (hPage -> GetData());

      auto mmHash = hash_fn_.GetHash(key);

      auto targetBlockIndex = mmHash % headerPage -> NumBlocks();
      auto blockPageId = headerPage -> GetBlockPageId(targetBlockIndex);

      Page * page = buffer_pool_manager_ -> FetchPage(blockPageId);
      if (page == nullptr) return false;
      HASH_TABLE_BLOCK_TYPE * blockPage = reinterpret_cast < HASH_TABLE_BLOCK_TYPE * > (page -> GetData());
      auto res = false;
      bool foundEmptySlot = false;
      size_t visitedBlocks = 1;
      while (!foundEmptySlot && visitedBlocks < headerPage -> NumBlocks()) {
        for (long unsigned int i = 0; i < BLOCK_ARRAY_SIZE; i++) {
           page->WLatch();
          if (blockPage -> IsValid(i) && comparator_(blockPage -> KeyAt(i), key) == 0 &&
            blockPage -> ValueAt(i) == value) {

            blockPage -> Remove(i);
            headerPage->count --;
            res = true;
            break;

          } else if (!blockPage -> IsOccupied(i)) {
            foundEmptySlot = true;
          }
           page->WUnlatch();

        }
        targetBlockIndex = (targetBlockIndex + 1) % headerPage -> NumBlocks();
        buffer_pool_manager_ -> UnpinPage(blockPageId, true);
        blockPageId = headerPage -> GetBlockPageId(targetBlockIndex);
        page = buffer_pool_manager_ -> FetchPage(blockPageId);
        blockPage = reinterpret_cast < HASH_TABLE_BLOCK_TYPE * > (page -> GetData());
        visitedBlocks++;
      }
      //unpin all page
      buffer_pool_manager_ -> UnpinPage(blockPageId, true);
      buffer_pool_manager_ -> UnpinPage(header_page_id_, true);
      this->table_latch_.RUnlock();
      return res;
    }

  /***************************
   * RESIZE
   ***************************/
  template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  this->table_latch_.WLock();
  auto header_page = this->HeaderPage();
  auto expected_size = initial_size * 2;
  // only grow up in size
  if (header_page->GetSize() < expected_size) {
    auto old_size = header_page->GetSize();
    header_page->SetSize(expected_size);
    testUse = true;
    this->appendBlocks(header_page, expected_size - old_size);
    // re-organize all key-value pairs
    auto all_old_blocks = std::vector<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>();
    auto all_block_page_ids = std::vector<page_id_t>();
    for (size_t idx = 0; idx < header_page->NumBlocks(); idx++) {
      all_old_blocks.push_back(this->BlockPage(header_page, idx));
      all_block_page_ids.push_back(header_page->GetBlockPageId(idx));
    }
    header_page->ResetNextIndex();
    for (size_t idx = 0; idx < header_page->NumBlocks(); idx++) {
      const auto &block = all_old_blocks[idx];
      for (size_t pair_idx = 0; pair_idx < BLOCK_ARRAY_SIZE; pair_idx++) {
        this->Insert(nullptr, block->KeyAt(pair_idx), block->ValueAt(pair_idx));
      }
      this->buffer_pool_manager_->DeletePage(all_block_page_ids[idx]);
    }
  }
  this->table_latch_.WUnlock();
}

  template < typename KeyType, typename ValueType, typename KeyComparator >
    size_t HASH_TABLE_TYPE::GetSize() {
      // auto size = headerPage->count;
      size_t size = this->HeaderPage()->GetSize();
      //buffer_pool_manager_ -> UnpinPage(header_page_id_, false);
      return size;

    }

    //For Tests Delete Me
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableHeaderPage *HASH_TABLE_TYPE::HeaderPage() {
  return reinterpret_cast<HashTableHeaderPage *>(
      this->buffer_pool_manager_->FetchPage(this->header_page_id_)->GetData());
  }
  template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::getTestUse() {
  return testUse;
  }
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableBlockPage<KeyType, ValueType, KeyComparator> *HASH_TABLE_TYPE::BlockPage(HashTableHeaderPage *header_page,
                                                                                  size_t bucket_ind) {
  return reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(
      this->buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(bucket_ind))->GetData());
}
template <typename KeyType, typename ValueType, typename KeyComparator>
slot_offset_t HASH_TABLE_TYPE::GetSlotIndex(const KeyType &key) {
  return this->hash_fn_.GetHash(key) % this->HeaderPage()->GetSize();
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::appendBlocks(HashTableHeaderPage *header_page, size_t num_buckets) {
  size_t total_current_buckets = header_page->NumBlocks() * BLOCK_ARRAY_SIZE;
  for (; total_current_buckets < num_buckets; total_current_buckets += BLOCK_ARRAY_SIZE) {
    page_id_t next_block_id;
    assert(this->buffer_pool_manager_->NewPage(&next_block_id) != nullptr);
    this->buffer_pool_manager_->UnpinPage(next_block_id, true);
    this->buffer_pool_manager_->FlushPage(next_block_id);
    header_page->AddBlockPageId(next_block_id);
  }
}
    //End

  template
  class LinearProbeHashTable < int, int, IntComparator > ;

  template
  class LinearProbeHashTable < GenericKey < 4 > , RID, GenericComparator < 4 >> ;

  template
  class LinearProbeHashTable < GenericKey < 8 > , RID, GenericComparator < 8 >> ;

  template
  class LinearProbeHashTable < GenericKey < 16 > , RID, GenericComparator < 16 >> ;

  template
  class LinearProbeHashTable < GenericKey < 32 > , RID, GenericComparator < 32 >> ;

  template
  class LinearProbeHashTable < GenericKey < 64 > , RID, GenericComparator < 64 >> ;

} // namespace bustub