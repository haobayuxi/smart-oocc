// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <map>
#include <unordered_map>

#include "common.h"
#include "cuckoohash_map.hh"

const offset_t NOT_FOUND = -1;

// For fast remote address lookup
class AddrCache {
 public:
  void Insert(node_id_t remote_node_id, table_id_t table_id, itemkey_t key,
              offset_t remote_offset) {
    // The node and table both exist, then insert/update the <key,offset> pair
    addr_map[remote_node_id][table_id].insert(key, remote_offset);
  }

  // We know which node to read, but we do not konw whether it is cached before
  ALWAYS_INLINE
  offset_t Search(node_id_t remote_node_id, table_id_t table_id,
                  itemkey_t key) {
    uint64_t offset = 0;
    if (addr_map[remote_node_id][table_id].find(key, offset)) {
      return (offset_t)offset;
    } else {
      return NOT_FOUND;
    }
    // auto offset_search = addr_map[remote_node_id][table_id].find(key);
    // return offset_search == addr_map[remote_node_id][table_id].end()
    //            ? NOT_FOUND
    //            : offset_search->second;
  }

  // size_t TotalAddrSize() {
  //   size_t total_size = 0;
  //   for (auto it = addr_map.begin(); it != addr_map.end(); it++) {
  //     total_size += sizeof(node_id_t);
  //     for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) {
  //       total_size += sizeof(table_id_t);
  //       for (auto it3 = it2->second.begin(); it3 != it2->second.end(); it3++)
  //       {
  //         total_size += (sizeof(itemkey_t) + sizeof(offset_t));
  //       }
  //     }
  //   }

  //   return total_size;
  // }

 private:
  // std::unordered_map<
  //     node_id_t,
  //     std::unordered_map<table_id_t, std::unordered_map<itemkey_t,
  //     offset_t>>> addr_map;

  // std::unordered_map<itemkey_t, offset_t> addr_map[1][20];
  libcuckoo::cuckoohash_map<int, uint64_t> addr_map[1][20];
};