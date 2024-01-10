
#pragma once

#include <cassert>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <vector>

#include "../memstore.h"
#include "fast_random.h"
#include "parse_config.h"
#include "util/generator.h"
#include "util/json_config.h"

using namespace benchmark;
using namespace std;

const int ObjectTableId = 0;
const int EdgeTableId = 1;

uint64_t getTimeNs() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);

  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

const int VALUE_SIZE = 150;
const int NUM_SHARDS = 50;

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

enum class EdgeType { Unique, Bidirectional, UniqueAndBidirectional, Other };

struct Edge {
  uint64_t primary_key;
  uint64_t remote_key;
  // EdgeType type;
};

class TAO {
 public:
  HashStore *object_table;
  HashStore *edge_table;
  std::vector<HashStore *> table_ptrs;
  ConfigParser config_parser;
  // std::unordered_map<int, std::vector<Edge>> const shard_to_edges;
  vector<Edge> shard_to_edges[NUM_SHARDS];

  uint64_t key_count;

  // ConfigParser::LineObject op_obj;
  TAO() {
    config_parser = ConfigParser();
    key_count = 0;
  }

  void LoadTable(MemStoreAllocParam *mem_store_alloc_param,
                 MemStoreReserveParam *mem_store_reserve_param) {
    object_table = new HashStore(ObjectTableId, 100000, mem_store_alloc_param);
    edge_table = new HashStore(EdgeTableId, 100000, mem_store_alloc_param);
    PopulateTable(mem_store_reserve_param);
    table_ptrs.push_back(object_table);
    table_ptrs.push_back(edge_table);
  }

  uint64_t GenerateKey(int shard) {
    uint64_t timestamp = getTimeNs();
    uint64_t seqnum = key_count++;
    // 64 bit int split into 7 bit shard, 17 thread-specific sequence number,
    // and bottom 40 bits of timestamp
    // this design is fairly arbitrary; intent is just to minimize duplicate
    // keys across threads
    return (((uint64_t)shard) << 57) + ((seqnum & 0x1FFFF) << 40) +
           (timestamp >> 24);
  }

  void PopulateTable(MemStoreReserveParam *mem_store_reserve_param) {
    std::uniform_int_distribution<> unif(0, NUM_SHARDS - 1);
    ConfigParser::LineObject &remote_shards =
        config_parser.fields["remote_shards"];
    uint8_t value[150] = {'a'};
    for (int i = 0; i < TOTAL_KEYS_NUM; i++) {
      int primary_shard = unif(gen);
      int remote_shard = remote_shards.distribution(gen);
      uint64_t primary_key = GenerateKey(primary_shard);
      uint64_t remote_key = GenerateKey(remote_shard);
      Edge e = Edge{
          primary_key,
          remote_key,
      };
      shard_to_edges[primary_shard].push_back(e);
      // insert object

      // insert edge

      DataItem item_to_be_inserted(ObjectTableId, 150, primary_key, &value);
      DataItem *inserted_item = object_table->LocalInsert(
          primary_key, item_to_be_inserted, mem_store_reserve_param);
      inserted_item->remote_offset =
          object_table->GetItemRemoteOffset(inserted_item);
    }
  }

  void GetReadTransactions() {
    ConfigParser::LineObject &read_transaction_size_obj =
        config_parser.fields["read_txn_sizes"];
    int transaction_size =
        read_transaction_size_obj
            .vals[read_transaction_size_obj.distribution(gen)];
    std::cout << "transaction size = " << transaction_size << std::endl;

    ConfigParser::LineObject &op_obj =
        config_parser.fields["read_txn_operation_types"];
    std::string op_type = op_obj.types[op_obj.distribution(gen)];
    // bool is
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

void GetRandomEdge() {}