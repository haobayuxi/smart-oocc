
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
#define TOTAL_EDGES_NUM 500000
#define TOTAO_OBJECT_NUM 1000
#define TOTAL_KEYS_NUM 1000000
uint64_t getTimeNs() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);

  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}

const int VALUE_SIZE = 10;

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
  bool read_only;
};

struct micro_key_t {
  // uint64_t micro_id;
  uint64_t item_key;
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

class TAO {
 public:
  HashStore *micro_table;
  HashStore *object_table;
  HashStore *edge_table;
  std::vector<HashStore *> table_ptrs;
  ConfigParser config_parser;
  // std::unordered_map<int, std::vector<Edge>> const shard_to_edges;
  vector<Edge> shard_to_edges[NUM_SHARDS + 1];
  vector<vector<tao_key_t>> query;
  uint64_t edge_count;
  uint64_t keys_per_shard;

  // ConfigParser::LineObject op_obj;
  TAO() {
    config_parser = ConfigParser();
    edge_count = 0;
    keys_per_shard = 20000;
    // PopulateData();
    // LoadEdges();
  }

  void GenerateQuery() {
    LoadEdges();
    for (int i = 0; i < 100000; i++) {
      bool is_read = is_read_transaction();

      if (is_read) {
        vector<tao_key_t> read_query = GetReadTransactions();
        // vector<tao_key_t> read;
        // read.push_back(tao_key_t{1, i, true});
        query.push_back(read_query);
      } else {
        vector<tao_key_t> write_query = GetWriteTransactions();
        query.push_back(write_query);
      }
      // cout << read_query[0].table_id << "  " << read_query[0].key << endl;
    }
  }

  void LoadEdges() {
    ifstream file("tao");
    uint64_t primary = 0;
    uint64_t remote = 0;
    for (int i = 0; i < TOTAL_EDGES_NUM; i++) {
      file >> primary;
      file >> remote;
      uint64_t shard = primary / keys_per_shard;
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
    // for (int i = 0; i < 50; i++) {
    //   cout << shard_to_edges[0][i].primary_key << "  "
    //        << shard_to_edges[0][i].remote_key << endl;
    // }
  }

  void LoadTable(MemStoreAllocParam *mem_store_alloc_param,
                 MemStoreReserveParam *mem_store_reserve_param) {
    object_table = new HashStore(ObjectTableId, 200000, mem_store_alloc_param);
    edge_table = new HashStore(EdgeTableId, 200000, mem_store_alloc_param);
    // PopulateTable(mem_store_reserve_param);
    PopulateObjectTable(mem_store_reserve_param);
    PopulateEdgeTable(mem_store_reserve_param);
    table_ptrs.push_back(object_table);
    table_ptrs.push_back(edge_table);
  }

  // uint64_t GenerateKey(int shard) {
  //   // uint64_t timestamp = getTimeNs();
  //   // // uint64_t seqnum = key_count++;
  //   // // 64 bit int split into 7 bit shard, 17 thread-specific sequence
  //   number,
  //   // // and bottom 40 bits of timestamp
  //   // // this design is fairly arbitrary; intent is just to minimize
  //   duplicate
  //   // // keys across threads
  //   // return (((uint64_t)shard) << 57) + ((timestamp << 24) >> 24);
  //   std::uniform_int_distribution<int> edge_selector(
  //       keys_per_shard * shard, keys_per_shard * (shard + 1) - 1);
  //   return edge_selector(gen);
  // }

  uint64_t GenerateEdgeKey(uint64_t primary_key, uint64_t remote_key) {
    // uint64_t shard = remote_key >> 57;
    // return primary_key + shard << 50;
    // return primary_key * 100000 + remote_key;
    return primary_key;
  }

  Edge GetRandomEdge() {
    ConfigParser::LineObject &obj = config_parser.fields["primary_shards"];
    int shard = obj.distribution(gen);
    std::uniform_int_distribution<int> shard_selector(0, 49);
    shard = shard_selector(gen);
    auto it = shard_to_edges[shard];
    std::uniform_int_distribution<int> edge_selector(0, it.size() - 1);
    int index = edge_selector(gen);

    return Edge{
        it[index].primary_key,
        it[index].remote_key,
    };
  }

  void PopulateObjectTable(MemStoreReserveParam *mem_store_reserve_param) {
    uint8_t value[VALUE_SIZE] = {'a'};
    ifstream file("tao");

    for (int i = 0; i < TOTAL_EDGES_NUM; i++) {
      uint64_t primary_key = 0;
      uint64_t remote_key = 0;
      file >> primary_key;
      file >> remote_key;
      DataItem item_to_be_inserted1(ObjectTableId, VALUE_SIZE,
                                    (itemkey_t)primary_key, value);
      DataItem *inserted_item1 = object_table->LocalInsert(
          primary_key, item_to_be_inserted1, mem_store_reserve_param);
      inserted_item1->remote_offset =
          object_table->GetItemRemoteOffset(inserted_item1);

      DataItem item_to_be_inserted2(ObjectTableId, VALUE_SIZE,
                                    (itemkey_t)remote_key, value);
      DataItem *inserted_item2 = object_table->LocalInsert(
          remote_key, item_to_be_inserted2, mem_store_reserve_param);
      inserted_item2->remote_offset =
          object_table->GetItemRemoteOffset(inserted_item2);
    }
    file.close();
  }

  void PopulateEdgeTable(MemStoreReserveParam *mem_store_reserve_param) {
    uint8_t value[VALUE_SIZE] = {'a'};
    ifstream file("tao");
    for (int i = 0; i < TOTAL_EDGES_NUM; i++) {
      uint64_t primary_key = 0;
      uint64_t remote_key = 0;
      file >> primary_key;
      file >> remote_key;
      // insert edge
      uint64_t edge_key = GenerateEdgeKey(primary_key, remote_key);
      DataItem item_to_be_inserted3(EdgeTableId, VALUE_SIZE,
                                    (itemkey_t)edge_key, value);
      DataItem *inserted_item3 = edge_table->LocalInsert(
          edge_key, item_to_be_inserted3, mem_store_reserve_param);
      inserted_item3->remote_offset =
          edge_table->GetItemRemoteOffset(inserted_item3);
    }
    file.close();
  }

  void PopulateData() {
    vector<uint64_t> key_per_shard;
    for (int i = 0; i <= NUM_SHARDS; i++) {
      key_per_shard.push_back(i * keys_per_shard);
    }
    std::uniform_int_distribution<> unif(0, NUM_SHARDS - 1);
    ConfigParser::LineObject &remote_shards =
        config_parser.fields["remote_shards"];
    ofstream file("tao");
    for (int i = 0; i < TOTAL_EDGES_NUM; i++) {
      int primary_shard = unif(gen);
      int remote_shard = remote_shards.distribution(gen);
      uint64_t primary_key = key_per_shard[primary_shard];
      uint64_t remote_key = key_per_shard[remote_shard];
      // uint64_t edge_key = GenerateEdgeKey(primary_key, remote_key);
      file << primary_key << endl;
      file << remote_key << endl;
      primary_key += 1;
      remote_key += 1;
      key_per_shard[primary_shard] = primary_key;
      key_per_shard[remote_shard] = remote_key;
    }
    file.close();
  }

  vector<tao_key_t> GetReadTransactions() {
    vector<tao_key_t> result;
    ConfigParser::LineObject &read_transaction_size_obj =
        config_parser.fields["read_txn_sizes"];
    int transaction_size =
        read_transaction_size_obj
            .vals[read_transaction_size_obj.distribution(gen)];
    while (transaction_size > 50) {
      transaction_size = read_transaction_size_obj
                             .vals[read_transaction_size_obj.distribution(gen)];
    }
    // std::cout << "transaction size = " << transaction_size << std::endl;
    ConfigParser::LineObject &op_obj =
        config_parser.fields["read_txn_operation_types"];
    // bool is
    // ConfigParser::LineObject &primary_shards =
    //     config_parser.fields["primary_shards"];
    // ConfigParser::LineObject &remote_shards =
    //     config_parser.fields["remote_shards"];
    // std::uniform_int_distribution<uint64_t> shard_selector(0, 49);
    for (int i = 0; i < transaction_size; i++) {
      // random a edge
      // random read edge or object
      Edge e = GetRandomEdge();
      int op = op_obj.distribution(gen);
      if (op == 1) {
        // read a edge
        result.push_back(tao_key_t{
            EdgeTableId,
            GenerateEdgeKey(e.primary_key, e.remote_key),
            // shard_selector(gen),
            true,
        });
      } else {
        // read a object
        result.push_back(tao_key_t{
            ObjectTableId,
            e.primary_key,
            true,
        });
      }
    }

    return result;
  }

  vector<tao_key_t> GetWriteTransactions() {
    vector<tao_key_t> result;
    ConfigParser::LineObject &read_transaction_size_obj =
        config_parser.fields["write_txn_sizes"];
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
        result.push_back(tao_key_t{
            EdgeTableId,
            GenerateEdgeKey(e.primary_key, e.remote_key),
            false,
        });
      } else {
        result.push_back(tao_key_t{
            ObjectTableId,
            e.primary_key,
            false,
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
