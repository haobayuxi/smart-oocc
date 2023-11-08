#include "dtx.h"

bool DTX::lease_expired(uint64_t lease) {
  if (lease > get_clock_sys_time_us()) {
    return false;
  }
  return true;
}

bool DTX::DrTMExeRO() {
  std::vector<CasRead> pending_cas_ro;
  std::vector<HashRead> pending_hash_ro;
  DrTMIssueReadOnly(pending_cas_ro, pending_hash_ro);
  context->Sync();
  std::list<InvisibleRead> pending_invisible_ro;
  std::list<HashRead> pending_next_hash_ro;
  if (!DrTMCheckDirectRO(pending_cas_ro, pending_invisible_ro,
                         pending_next_hash_ro))
    return false;
  if (!DrTMCheckHashRO(pending_hash_ro, pending_invisible_ro,
                       pending_next_hash_ro))
    return false;
  while (!pending_invisible_ro.empty() || !pending_next_hash_ro.empty()) {
    context->Sync();
    // if (!CheckInvisibleRO(pending_invisible_ro)) return false;
    if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro))
      return false;
  }
  return true;
}

bool DTX::DrTMCheckDirectRO(std::vector<CasRead> &pending_cas_ro,
                            std::list<InvisibleRead> &pending_invisible_ro,
                            std::list<HashRead> &pending_next_hash_ro) {
  for (auto &res : pending_cas_ro) {
    auto *it = res.item->item_ptr.get();
    auto *fetched_item = (DataItem *)res.buf;
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (likely(fetched_item->valid)) {
        *it = *fetched_item;
        res.item->is_fetched = true;
        if (it->lock % 2 == 1) {
          // write locked
          return false;
        } else {
          auto lease = ite->lock >> 1;
          if (lease_expired(lease)) {
            return false;
          }
        }

      } else {
        addr_cache->Insert(res.node_id, it->table_id, it->key, NOT_FOUND);
        return false;
      }
    } else {
      node_id_t remote_node_id = GetPrimaryNodeID(it->table_id);
      const HashMeta &meta = GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      auto *local_hash_node = (HashNode *)AllocLocalBuffer(sizeof(HashNode));
      pending_next_hash_ro.emplace_back(HashRead{.node_id = remote_node_id,
                                                 .item = res.item,
                                                 .buf = (char *)local_hash_node,
                                                 .meta = meta});
      context->read((char *)local_hash_node,
                    GlobalAddress(remote_node_id, node_off), sizeof(HashNode));
    }
  }
  return true;
}

bool DTX::DrTMCheckHashRO(std::vector<HashRead> &pending_hash_ro,
                          std::list<InvisibleRead> &pending_invisible_ro,
                          std::list<HashRead> &pending_next_hash_ro) {
  for (auto &res : pending_hash_ro) {
    auto *local_hash_node = (HashNode *)res.buf;
    auto *it = res.item->item_ptr.get();
    bool find = false;

    for (auto &item : local_hash_node->data_items) {
      if (item.valid && item.key == it->key && item.table_id == it->table_id) {
        *it = item;
        addr_cache->Insert(res.node_id, it->table_id, it->key,
                           it->remote_offset);
        res.item->is_fetched = true;
        find = true;
        break;
      }
    }

    if (likely(find)) {
      if (it->lock % 2 == 1) {
        // write locked
        return false;
      } else {
        auto lease = ite->lock >> 1;
        if (lease_expired(lease)) {
          return false;
        }
      }
    } else {
      if (local_hash_node->next == nullptr) return false;
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      pending_next_hash_ro.emplace_back(HashRead{.node_id = res.node_id,
                                                 .item = res.item,
                                                 .buf = res.buf,
                                                 .meta = res.meta});
      context->read(res.buf, GlobalAddress(res.node_id, node_off),
                    sizeof(HashNode));
    }
  }
  return true;
}

// bool DTX::CheckNextHashRO(std::list<InvisibleRead> &pending_invisible_ro,
//                           std::list<HashRead> &pending_next_hash_ro) {
//   for (auto iter = pending_next_hash_ro.begin();
//        iter != pending_next_hash_ro.end();) {
//     auto res = *iter;
//     auto *local_hash_node = (HashNode *)res.buf;
//     auto *it = res.item->item_ptr.get();
//     bool find = false;

//     for (auto &item : local_hash_node->data_items) {
//       if (item.valid && item.key == it->key && item.table_id == it->table_id)
//       {
//         *it = item;
//         addr_cache->Insert(res.node_id, it->table_id, it->key,
//                            it->remote_offset);
//         res.item->is_fetched = true;
//         find = true;
//         break;
//       }
//     }

//     if (likely(find)) {
//       if (unlikely((it->lock & STATE_INVISIBLE))) {
//         char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
//         uint64_t lock_offset = it->GetRemoteLockAddr(it->remote_offset);
//         pending_invisible_ro.emplace_back(InvisibleRead{
//             .node_id = res.node_id, .buf = cas_buf, .off = lock_offset});
//         context->read(cas_buf, GlobalAddress(res.node_id, lock_offset),
//                       sizeof(lock_t));
//       }
//       iter = pending_next_hash_ro.erase(iter);
//     } else {
//       if (local_hash_node->next == nullptr) return false;
//       auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
//                       res.meta.base_off;
//       context->read(res.buf, GlobalAddress(res.node_id, node_off),
//                     sizeof(HashNode));
//       iter++;
//     }
//   }
//   return true;
// }

bool DTX::DrTMIssueReadOnly(std::vector<CasRead> &pending_cas_ro,
                            std::vector<HashRead> &pending_hash_ro) {
  uint64_t read_lease = (start_time + 1000) << 1;
  for (auto &item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    node_id_t node_id = GetPrimaryNodeID(it->table_id);
    item.read_which_node = node_id;
    auto offset = addr_cache->Search(node_id, it->table_id, it->key);
    if (offset != NOT_FOUND) {
      it->remote_offset = offset;
      char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
      char *data_buf = AllocLocalBuffer(DataItemSize);
      pending_cas_ro.emplace_back(CasRead{
          .item = &read_write_set[i],
          .cas_buf = cas_buf,
          .data_buf = data_buf,
          .node_id = node_id,
      });
      context->CompareAndSwap(cas_buf,
                              GlobalAddress(node_id, GetRemoteLockAddr(offset)),
                              0, read_lease);
      context->read(data_buf, GlobalAddress(node_id, offset), DataItemSize);
      context->PostRequest();
    } else {
      HashMeta meta = GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      char *buf = AllocLocalBuffer(sizeof(HashNode));
      pending_hash_ro.emplace_back(HashRead{
          .node_id = node_id, .item = &item, .buf = buf, .meta = meta});
      context->read(buf, GlobalAddress(node_id, node_off), sizeof(HashNode));
      context->PostRequest();
    }
  }
  return true;
}

bool DTX::DrTMCommit() {
  // free read locks
  for (auto &item : read_only_set) {
  }
  // free write locks
  for (auto &rw : read_write_set) {
  }
  return true;
}