
#pragma once

#include <cassert>
#include <cstdint>
#include <iostream>
#include <vector>

#include "../memstore.h"
#include "fast_random.h"
#include "parse_config.h"
#include "util/generator.h"
#include "util/json_config.h"

using namespace benchmark;

const int VALUE_SIZE = 150;

static inline unsigned long GetCPUCycle() {
  unsigned a, d;
  __asm __volatile("rdtsc" : "=a"(a), "=d"(d));
  return ((unsigned long)a) | (((unsigned long)d) << 32);
}

thread_local static std::mt19937 gen(std::random_device{}());
thread_local static std::independent_bits_engine<std::default_random_engine, 8,
                                                 unsigned char>
    byte_engine;

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

class TAO {
 public:
  HashStore *micro_table;
  std::vector<HashStore *> table_ptrs;
  ConfigParser config_parser;

  TAO() { config_parser = ConfigParser(); }

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

      DataItem item_to_be_inserted(MICRO_TABLE_ID, sizeof(micro_val_t),
                                   micro_key.item_key, (uint8_t *)&micro_val);
      DataItem *inserted_item = micro_table->LocalInsert(
          micro_key.item_key, item_to_be_inserted, mem_store_reserve_param);
      inserted_item->remote_offset =
          micro_table->GetItemRemoteOffset(inserted_item);
    }
  }

  void GetReadTransactions() {
    ConfigParser::LineObject &obj = config_parser.fields["read_txn_sizes"];
    int transaction_size = obj.vals[obj.distribution(gen)];
    std::cout << "transaction size = " << transaction_size << std::endl;
    for (int i = 0; i < transaction_size; i++) {
      // random a edge
      // random read edge or object
      bool is_edge_op = true;
      if (is_edge_op) {
        // read a edge
      } else {
        // read a object
      }
    }
  }

  void GetWriteTransactions() {}

  ALWAYS_INLINE
  std::vector<HashStore *> GetHashStore() { return table_ptrs; }
};