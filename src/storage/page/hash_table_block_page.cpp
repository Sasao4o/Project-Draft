//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
 bool HASH_TABLE_BLOCK_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsFull()) {
    return false;
  }
if (isKeyValueExist(key,value, cmp)) {
  return false;
}
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsReadable(i)) {
      array_[i] = MappingType(key, value);
      SetReadable(i, 1);
      SetOccupied(i, 1);
      break;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
 bool HASH_TABLE_BLOCK_TYPE::isKeyValueExist(KeyType key, ValueType value, KeyComparator cmp) {
 std::vector<ValueType> result;
  GetValue(key, cmp, &result);
  if (std::find(result.cbegin(), result.cend(), value) != result.cend()) {
    return true;
  }
  return false;
 }
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
    if (IsOccupied(bucket_ind)) {
        return false;
    }

    array_[bucket_ind] = {key, value};

    SetOccupied(bucket_ind , 1);
    SetReadable(bucket_ind , 1);
    return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  SetOccupied(bucket_ind, 1);
  SetReadable(bucket_ind, 0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
     auto location = GetLocation(bucket_ind);

    return (occupied_[location.first] & (1 << location.second)) != 0;

}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
      auto location = GetLocation(bucket_ind);

    return (readable_[location.first] & (1 << location.second)) != 0;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
char HASH_TABLE_BLOCK_TYPE::GetMask(int which, int bit) const {
    char mask = 0;
    if (bit == 0) {
      mask = static_cast<char>(~(1 << which));
    } else {
      mask = static_cast<char>(1 << which);
    }
    return mask;
  }

template <typename KeyType, typename ValueType, typename KeyComparator>
  std::pair<int, int> HASH_TABLE_BLOCK_TYPE::GetLocation(uint32_t bucket_idx) const {
    auto ret = std::pair<int, int>(0, 0);
    ret.first = bucket_idx / 8;
    ret.second = bucket_idx % 8;
    return ret;
  }

  template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::SetOccupied(uint32_t bucket_idx, int bit) {
  auto location = GetLocation(bucket_idx);
  char mask = GetMask(location.second, bit);
  if (bit == 0) {
    occupied_[location.first] &= mask;
  } else {
    occupied_[location.first] |= mask;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::SetReadable(uint32_t bucket_idx, int bit) {
  auto location = GetLocation(bucket_idx);
  char mask = GetMask(location.second, bit);
  if (bit == 0) {
    readable_[location.first] &= mask;
  } else {
    readable_[location.first] |= mask;
  }
}


template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (uint32_t i = 0; (int)(i < BLOCK_ARRAY_SIZE); i++) {
    if (!IsReadable(i)) {
      if (!IsOccupied(i)) {
        break;
      }
      continue;
    }
    if (cmp(key, KeyAt(i)) == 0) {
      result->push_back(ValueAt(i));
    }
  }
  return !result->empty();
}
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BLOCK_TYPE::NumReadable() {
  uint32_t num_readable = 0;
  for (int i = 0; i < (int)((BLOCK_ARRAY_SIZE - 1) / 8 + 1); i++) {
    uint8_t readable = readable_[i];
    while (readable != 0) {
      readable &= (readable - 1);
      num_readable++;
    }
  }
  return num_readable;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsFull() {
  return NumReadable() == BLOCK_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsEmpty() {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsValid(slot_offset_t bucket_ind) const {
  return IsOccupied(bucket_ind) && IsReadable(bucket_ind);
}
//THIS LINES ARE OUTSIDE THE CLASS BECAREFULLL!!!!! (5LE BALK)
// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub