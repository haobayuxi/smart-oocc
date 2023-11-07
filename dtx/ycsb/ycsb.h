#pragma once

#include <cassert>
#include <cstdint>
#include <vector>

#include "../memstore.h"
#include "util/json_config.h"
#include "zipf.h"

static inline unsigned long GetCPUCycle() {
  unsigned a, d;
  __asm __volatile("rdtsc" : "=a"(a), "=d"(d));
  return ((unsigned long)a) | (((unsigned long)d) << 32);
}

#define TOTAL_KEYS_NUM 50000

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

class YCSB {
 public:
  HashStore *micro_table;
  std::vector<HashStore *> table_ptrs;
  YCSB(double theta, int thread_gid) {
    uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();
    uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
    zipf_gen = new ZipfGen(TOTAL_KEYS_NUM, theta, zipf_seed & zipf_seed_mask);
  }

  void LoadTable(MemStoreAllocParam *mem_store_alloc_param,
                 MemStoreReserveParam *mem_store_reserve_param) {
    micro_table =
        new HashStore(MICRO_TABLE_ID, TOTAL_KEYS_NUM, mem_store_alloc_param);
    PopulateTable(mem_store_reserve_param);
    table_ptrs.push_back(micro_table);
  }

  void PopulateTable(MemStoreReserveParam *mem_store_reserve_param) {
    for (int i = 0; i < TOTAL_KEYS_NUM; i++) {
      micro_key_t micro_key;
      micro_key.item_key = (uint64_t)i;

      micro_val_t micro_val;
      micro_val.magic = micro_magic + i;

      DataItem item_to_be_inserted(MICRO_TABLE_ID, sizeof(micro_val_t),
                                   micro_key.item_key, (uint8_t *)&micro_val);
      DataItem *inserted_item = micro_table->LocalInsert(
          micro_key.item_key, item_to_be_inserted, mem_store_reserve_param);
      inserted_item->remote_offset =
          micro_table->GetItemRemoteOffset(inserted_item);
    }
  }
  ALWAYS_INLINE
  itemkey_t next() { return (itemkey_t)(zipf_gen->next()); }

  ALWAYS_INLINE
  std::vector<HashStore *> GetHashStore() { return table_ptrs; }

 private:
  ZipfGen *zipf_gen;
};