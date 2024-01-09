
#pragma once

#include <cassert>
#include <cstdint>
#include <iostream>
#include <vector>

#include "../memstore.h"
#include "fast_random.h"
#include "util/generator.h"
#include "util/json_config.h"

const int VALUE_SIZE = 150;

#pragma once

// [
//   94036, 36224, 3600, 612, 612, 612, 612, 612, 614, 612, 612, 612, 612, 612, 612, 612, 614,
//   612,   612,   612,  612, 612, 612, 612, 612, 614, 612, 612, 612, 612, 612, 612, 612, 614,
//   612,   612,   612,  612, 612, 612, 612, 614, 612, 612, 612, 612, 612, 612, 612, 600
// ];
static inline unsigned long GetCPUCycle() {
  unsigned a, d;
  __asm __volatile("rdtsc" : "=a"(a), "=d"(d));
  return ((unsigned long)a) | (((unsigned long)d) << 32);
}

#define TOTAL_KEYS_NUM 1000000

const int MICRO_TABLE_ID = 1;

struct micro_key_t {
  // uint64_t micro_id;
  uint64_t item_key;
};

static_assert(sizeof(micro_key_t) == sizeof(uint64_t), "");

struct micro_val_t {
  uint64_t magic;
};
// static_assert(sizeof(micro_val_t) == 40, "");

// Magic numbers for debugging. These are unused in the spec.
#define Micro_MAGIC 97 /* Some magic number <= 255 */
#define micro_magic (Micro_MAGIC)

static ALWAYS_INLINE uint64_t align_pow2(uint64_t v) {
  v--;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  return v + 1;
}

enum ReadTYPE : int {
  obj_read = 0,
  edge_point_read = 1,
};
const int ReadTypeWeight[] = [ 1, 5 ];

enum WriteType : int {
  obj_add = 0,
  obj_update = 1,
  edge_add = 2,
  edge_update = 3,
};
const int WriteTypeWeight[] = [ 111, 214, 10, 380 ];

class TAO {
 public:
  HashStore *micro_table;
  std::vector<HashStore *> table_ptrs;
  int transaction_size;
  std::discrete_distribution<> distribution;
  std::vector<double> weights;

  void LoadTable(MemStoreAllocParam *mem_store_alloc_param,
                 MemStoreReserveParam *mem_store_reserve_param) {
    micro_table = new HashStore(MICRO_TABLE_ID, 200000, mem_store_alloc_param);
    PopulateTable(mem_store_reserve_param);
    table_ptrs.push_back(micro_table);
  }

  void PopulateTable(MemStoreReserveParam *mem_store_reserve_param) {
    for (int i = 0; i < TOTAL_KEYS_NUM; i++) {
      micro_key_t micro_key;
      micro_key.item_key = (uint64_t)i;

      micro_val_t micro_val;
      micro_val.magic = micro_magic + i;

      DataItem item_to_be_inserted(MICRO_TABLE_ID, sizeof(micro_val_t), micro_key.item_key,
                                   (uint8_t *)&micro_val);
      DataItem *inserted_item = micro_table->LocalInsert(micro_key.item_key, item_to_be_inserted,
                                                         mem_store_reserve_param);
      inserted_item->remote_offset = micro_table->GetItemRemoteOffset(inserted_item);
    }
  }

  void GetReadTransactions() {
    for (int i = 0; i < transaction_size; i++) {
    }
  }

  void GetWriteTransactions() {}

  ALWAYS_INLINE
  std::vector<HashStore *> GetHashStore() { return table_ptrs; }
};
