
#pragma once

#include <cassert>
#include <cstdint>
#include <ctime>
#include <fstream>
#include <iostream>
#include <vector>

#include "../memstore.h"
#include "fast_random.h"
#include "parse_config.h"
#include "util/generator.h"
#include "util/json_config.h"

using namespace benchmark;
using namespace std;

const int ObjectTableId = 1;
const int EdgeTableId = 2;
#define TOTAL_EDGES_NUM 2000000

uint64_t getTimeNs() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);

  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

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

const int MICRO_TABLE_ID = 1;

struct tao_key_t {
  int table_id;
  uint64_t key;
};

struct micro_val_t {
  uint64_t magic;
};

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

class TAO {
 public:
  HashStore *object_table;
  HashStore *edge_table;
  std::vector<HashStore *> table_ptrs;
  ConfigParser config_parser;
  // std::unordered_map<int, std::vector<Edge>> const shard_to_edges;
  vector<Edge> shard_to_edges[NUM_SHARDS + 1];

  uint64_t edge_count;

  // ConfigParser::LineObject op_obj;
  TAO() {
    config_parser = ConfigParser();
    edge_count = 0;
    // LoadEdges();
    // cout << "load edges done=" << edge_count << endl;
  }

  void LoadEdges() {
    ifstream file("tao.dat");
    uint64_t primary = 0;
    uint64_t remote = 0;
    for (int i = 0; i < TOTAL_EDGES_NUM; i++) {
      file >> primary;
      file >> remote;
      uint64_t shard = primary >> 57;
      if (shard > 50) {
        continue;
      }
      shard_to_edges[shard].push_back(Edge{
          primary,
          remote,
      });
      edge_count++;
    }
    file.close();
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
    // uint64_t seqnum = key_count++;
    // 64 bit int split into 7 bit shard, 17 thread-specific sequence number,
    // and bottom 40 bits of timestamp
    // this design is fairly arbitrary; intent is just to minimize duplicate
    // keys across threads
    return (((uint64_t)shard) << 57) + ((timestamp << 24) >> 24);
  }

  uint64_t GenerateEdgeKey(uint64_t primary_key, uint64_t remote_key) {
    uint64_t shard = remote_key >> 57;
    return primary_key + shard << 50;
  }

  Edge const &GetRandomEdge() {
    ConfigParser::LineObject &obj = config_parser.fields["primary_shards"];
    auto it = shard_to_edges[obj.distribution(
        gen)];  // 从 primary shard 拿到shard的key

    std::uniform_int_distribution<int> edge_selector(0, it.size() - 1);
    return it[edge_selector(gen)];
  }

  void PopulateTable(MemStoreReserveParam *mem_store_reserve_param) {
    std::uniform_int_distribution<> unif(0, NUM_SHARDS - 1);
    ConfigParser::LineObject &remote_shards =
        config_parser.fields["remote_shards"];
    uint8_t value[150] = {'a'};
    ofstream file("tao.dat");
    // ifstream file("tao.dat");
    for (int i = 0; i < TOTAL_EDGES_NUM; i++) {
      int primary_shard = unif(gen);
      int remote_shard = remote_shards.distribution(gen);
      uint64_t primary_key = GenerateKey(primary_shard);
      uint64_t remote_key = GenerateKey(remote_shard);
      // uint64_t primary_key = 0;
      // uint64_t remote_key = 0;
      // file >> primary_key;
      // file >> remote_key;
      // Edge e = Edge{
      //     primary_key,
      //     remote_key,
      // };
      // // shard_to_edges[primary_shard].push_back(e);
      // // edge_count++;
      // // insert object
      // DataItem item_to_be_inserted1(ObjectTableId, 150,
      // (itemkey_t)primary_key,
      //                               value);
      // DataItem *inserted_item1 = object_table->LocalInsert(
      //     primary_key, item_to_be_inserted1, mem_store_reserve_param);
      // inserted_item1->remote_offset =
      //     object_table->GetItemRemoteOffset(inserted_item1);

      // DataItem item_to_be_inserted2(ObjectTableId, 150,
      // (itemkey_t)remote_key,
      //                               value);
      // DataItem *inserted_item2 = object_table->LocalInsert(
      //     remote_key, item_to_be_inserted2, mem_store_reserve_param);
      // inserted_item2->remote_offset =
      //     object_table->GetItemRemoteOffset(inserted_item2);
      // // insert edge
      // uint64_t edge_key = GenerateEdgeKey(primary_key, remote_key);
      // DataItem item_to_be_inserted3(EdgeTableId, 150, (itemkey_t)edge_key,
      //                               value);
      // DataItem *inserted_item3 = edge_table->LocalInsert(
      //     edge_key, item_to_be_inserted3, mem_store_reserve_param);
      // inserted_item3->remote_offset =
      //     edge_table->GetItemRemoteOffset(inserted_item3);
      file << primary_key << endl;
      file << remote_key << endl;
    }
    file.close();

    // cout << "pri" << primary_key << "remote" << remote_key << endl;
  }

  vector<tao_key_t> GetReadTransactions() {
    vector<tao_key_t> result;
    ConfigParser::LineObject &read_transaction_size_obj =
        config_parser.fields["read_txn_sizes"];
    int transaction_size =
        read_transaction_size_obj
            .vals[read_transaction_size_obj.distribution(gen)];
    // std::cout << "transaction size = " << transaction_size << std::endl;
    while (transaction_size > 50) {
      transaction_size = read_transaction_size_obj
                             .vals[read_transaction_size_obj.distribution(gen)];
    }
    ConfigParser::LineObject &op_obj =
        config_parser.fields["read_txn_operation_types"];
    // bool is
    for (int i = 0; i < transaction_size; i++) {
      // random a edge
      // random read edge or object
      std::string op_type = op_obj.types[op_obj.distribution(gen)];
      bool is_edge_op = true;
      Edge e = GetRandomEdge();
      if (is_edge_op) {
        // read a edge
        result.push_back(tao_key_t{
            EdgeTableId,
            GenerateEdgeKey(e.primary_key, e.remote_key),
        });
      } else {
        // read a object
        result.push_back(tao_key_t{
            ObjectTableId,
            e.primary_key,
        });
      }
    }

    return result;
  }

  vector<tao_key_t> GetWriteTransactions() {
    vector<tao_key_t> result;
    ConfigParser::LineObject &read_transaction_size_obj =
        config_parser.fields["read_txn_sizes"];
    int transaction_size =
        read_transaction_size_obj
            .vals[read_transaction_size_obj.distribution(gen)];
    while (transaction_size > 20) {
      transaction_size = read_transaction_size_obj
                             .vals[read_transaction_size_obj.distribution(gen)];
    }
    ConfigParser::LineObject &obj =
        config_parser.fields["write_txn_operation_types"];

    for (int i = 0; i < transaction_size; i++) {
      string operation_type = obj.types[obj.distribution(gen)];
      bool is_edge_op = operation_type.find("edge") != std::string::npos;
      Edge e = GetRandomEdge();
      if (is_edge_op) {
        // read a edge
        result.push_back(tao_key_t{
            EdgeTableId,
            GenerateEdgeKey(e.primary_key, e.remote_key),
        });
      } else {
        // read a object
        result.push_back(tao_key_t{
            ObjectTableId,
            e.primary_key,
        });
      }
    }
    return result;
  }

  bool is_read_transaction() {
    std::discrete_distribution<> op_dist =
        config_parser.fields["operations"].distribution;
    if (op_dist(gen) == 0) {
      return true;
    }
    return false;
  }

  ALWAYS_INLINE
  std::vector<HashStore *> GetHashStore() { return table_ptrs; }
};
