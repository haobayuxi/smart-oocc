
#include "dtx.h"

#define COUNT_MAX 32768

//    | max x| max s| n x| n s|  lock representation

#define nx_mask 0xF000
#define ns_mask 0x0F00
#define max_x_mask 0x00F0
#define max_s_mask 0x000F

#define acquire_read_lock 0x0100
#define acquire_write_lock 0x1000
#define release_read_lock 0x0001
#define realese_write_lock 0x0010

#define max_s_minus1 0xFF00
#define max_x_minus1 0xF000

uint64_t get_max_x(uint64_t lock) {
  auto nx = lock | nx_mask;
  return nx >> 48;
}

uint64_t get_max_s(uint64_t lock) {
  auto ns = lock | ns_mask;
  return ns >> 32;
}

uint64_t get_nx(uint64_t lock) {
  auto max_x = lock | max_x_mask;
  return max_x >> 16;
}

uint64_t get_ns(uint64_t lock) { return lock | max_s_mask; }

bool check_write_lock(uint64_t lock) {
  if (get_ns(lock) == get_max_s(lock)) {
    return true;
  }
  return false;
}

bool check_read_lock(uint64_t lock) {
  if (get_nx(lock) == get_max_x(lock)) {
    return true;
  }
  return false;
}

bool DTX::DSLRExeRO() {
  std::vector<CasRead> pending_cas_ro;
  std::vector<HashRead> pending_hash_ro;
  DSLRIssueReadOnly(pending_cas_ro, pending_hash_ro);
  context->Sync();
  std::list<CasRead> pending_next_cas_ro;
  std::list<DirectRead> pending_next_direct_ro;
  std::list<InvisibleRead> pending_invisible_ro;
  std::list<HashRead> pending_next_hash_ro;
  if (!DSLRCheckDirectRO(pending_cas_ro, pending_next_cas_ro,
                         pending_next_hash_ro))
    return false;
  if (!DSLRCheckHashRO(pending_hash_ro, pending_next_cas_ro,
                       pending_next_hash_ro))
    return false;
  for (int i = 0; i < 500; i++) {
    if (!pending_invisible_ro.empty() || !pending_next_cas_ro.empty() ||
        !pending_next_hash_ro.empty()) {
      context->Sync();
      if (!CheckInvisibleRO(pending_invisible_ro)) return false;
      if (!DSLRCheckNextCasRO(pending_next_cas_ro)) return false;
      if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro))
        return false;
    } else {
      break;
    }
  }
  return true;
}

bool DTX::DSLRExeRW() {
  is_ro_tx = false;
  std::vector<CasRead> pending_cas_ro;
  std::vector<HashRead> pending_hash_ro;
  std::vector<CasRead> pending_cas_rw;
  std::vector<HashRead> pending_hash_rw;
  std::vector<InsertOffRead> pending_insert_off_rw;
  std::list<CasRead> pending_next_cas_ro;
  std::list<CasRead> pending_next_cas_rw;
  std::list<InvisibleRead> pending_invisible_ro;
  std::list<HashRead> pending_next_hash_ro;
  std::list<HashRead> pending_next_hash_rw;
  std::list<InsertOffRead> pending_next_off_rw;
  DSLRIssueReadOnly(pending_cas_ro, pending_hash_ro);
  DSLRIssueReadWrite(pending_cas_rw, pending_hash_rw, pending_insert_off_rw);
  context->Sync();
  if (!DSLRCheckDirectRO(pending_cas_ro, pending_next_cas_ro,
                         pending_next_hash_ro))
    return false;
  if (!DSLRCheckHashRO(pending_hash_ro, pending_next_cas_ro,
                       pending_next_hash_ro))
    return false;
  if (!DSLRCheckHashRW(pending_hash_rw, pending_invisible_ro,
                       pending_next_hash_rw))
    return false;
  if (!CheckInsertOffRW(pending_insert_off_rw, pending_invisible_ro,
                        pending_next_off_rw))
    return false;
  if (!DSLRCheckCasRW(pending_cas_rw, pending_next_hash_rw,
                      pending_next_off_rw))
    return false;
  for (int i = 0; i < 100; i++) {
    if (!pending_invisible_ro.empty() || !pending_next_hash_ro.empty() ||
        !pending_next_hash_rw.empty() || !pending_next_off_rw.empty()) {
      context->Sync();
      if (!CheckInvisibleRO(pending_invisible_ro)) return false;
      if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro))
        return false;
      if (!DSLRCheckNextHashRW(pending_invisible_ro, pending_next_hash_rw))
        return false;
      if (!CheckNextOffRW(pending_invisible_ro, pending_next_off_rw))
        return false;
    } else {
      break;
    }
  }
  //   while (!pending_invisible_ro.empty() || !pending_next_hash_ro.empty() ||
  //          !pending_next_hash_rw.empty() || !pending_next_off_rw.empty()) {
  //     context->Sync();
  //     if (!CheckInvisibleRO(pending_invisible_ro)) return false;
  //     if (!CheckNextHashRO(pending_invisible_ro, pending_next_hash_ro))
  //       return false;
  //     if (!DrTMCheckNextHashRW(pending_invisible_ro, pending_next_hash_rw))
  //       return false;
  //     if (!CheckNextOffRW(pending_invisible_ro, pending_next_off_rw))
  //       return false;
  //   }
  ParallelUndoLog();
  return true;
}

bool DTX::DSLRIssueReadWrite(
    std::vector<CasRead> &pending_cas_rw,
    std::vector<HashRead> &pending_hash_rw,
    std::vector<InsertOffRead> &pending_insert_off_rw) {
  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i].is_fetched) continue;
    auto it = read_write_set[i].item_ptr;
    auto node_id = GetPrimaryNodeID(it->table_id);
    read_write_set[i].read_which_node = node_id;
    auto offset = addr_cache->Search(node_id, it->table_id, it->key);
    if (offset != NOT_FOUND) {
      it->remote_offset = offset;
      locked_rw_set.emplace_back(i);
      char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
      char *data_buf = AllocLocalBuffer(DataItemSize);
      pending_cas_rw.emplace_back(CasRead{.node_id = node_id,
                                          .item = &read_write_set[i],
                                          .cas_buf = cas_buf,
                                          .data_buf = data_buf});
      context->CompareAndSwap(
          cas_buf, GlobalAddress(node_id, it->GetRemoteLockAddr(offset)),
          STATE_CLEAN, STATE_LOCKED);
      context->read(data_buf, GlobalAddress(node_id, offset), DataItemSize);
      context->PostRequest();
    } else {
      not_eager_locked_rw_set.emplace_back(i);
      const HashMeta &meta = GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      char *local_hash_node = AllocLocalBuffer(sizeof(HashNode));
      if (it->user_insert) {
        pending_insert_off_rw.emplace_back(
            InsertOffRead{.node_id = node_id,
                          .item = &read_write_set[i],
                          .buf = local_hash_node,
                          .meta = meta,
                          .node_off = node_off});
      } else {
        pending_hash_rw.emplace_back(HashRead{.node_id = node_id,
                                              .item = &read_write_set[i],
                                              .buf = local_hash_node,
                                              .meta = meta});
      }
      context->read(local_hash_node, GlobalAddress(node_id, node_off),
                    sizeof(HashNode));
      context->PostRequest();
    }
  }
  return true;
}

bool DTX::DSLRCheckNextCasRO(std::list<CasRead> &pending_next_cas_ro) {
  for (auto iter = pending_next_cas_ro.begin();
       iter != pending_next_cas_ro.end(); iter++) {
    auto res = *iter;
    auto *it = res.item->item_ptr.get();
    auto *fetched_item = (DataItem *)res.data_buf;
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (likely(fetched_item->valid)) {
        *it = *fetched_item;
        res.item->is_fetched = true;
        if (it->lock % 2 == 1) {
          // write locked
          return false;
        } else {
          if (!lease_expired(it->lock)) {
            // retry
            context->CompareAndSwap(
                res.cas_buf,
                GlobalAddress(res.node_id, it->GetRemoteLockAddr(
                                               fetched_item->remote_offset)),
                it->lock, next_lease());
            context->read(
                res.data_buf,
                GlobalAddress(res.node_id, fetched_item->remote_offset),
                DataItemSize);
            context->PostRequest();
          } else {
            iter = pending_next_cas_ro.erase(iter);
          }
        }
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  return true;
}

bool DTX::DSLRCheckCasRO(std::vector<CasRead> &pending_cas_ro,
                         std::list<DirectRead> &pending_next_direct_ro,
                         std::list<HashRead> &pending_next_hash_ro) {
  for (auto &res : pending_cas_ro) {
    auto *it = res.item->item_ptr.get();
    auto *fetched_item = (DataItem *)res.data_buf;
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (likely(fetched_item->valid)) {
        *it = *fetched_item;
        res.item->is_fetched = true;
        if (!check_read_lock(it->lock)) {
          // write locked
          char *data_buf = AllocLocalBuffer(DataItemSize);
          pending_next_direct_ro.emplace_back(CasRead{
              .node_id = res.node_id,
              .item = res.item,
              .cas_buf = cas_buf,
              .data_buf = data_buf,
          });
          context->read(data_buf,
                        GlobalAddress(res.node_id, fetched_item->remote_offset),
                        DataItemSize);
          context->PostRequest();
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

bool DTX::DSLRCheckDirectRO(std::list<DirectRead> &pending_next_direct_ro) {
  for (auto iter = pending_next_direct_ro.begin();
       iter != pending_next_direct_ro.end(); iter++) {
    auto res = *iter;
    auto *it = res.item_per.get();
    if (!check_read_lock(it->lock)) {
      // write locked
      char *data_buf = AllocLocalBuffer(DataItemSize);
      pending_next_direct_ro.emplace_back(CasRead{
          .node_id = res.node_id,
          .item = res.item,
          .cas_buf = cas_buf,
          .data_buf = data_buf,
      });
      context->read(data_buf,
                    GlobalAddress(res.node_id, fetched_item->remote_offset),
                    DataItemSize);
      context->PostRequest();
      iter = pending_next_direct_ro.erase(iter);
    }
  }
  return true;
}

bool DTX::DSLRCheckHashRO(std::vector<HashRead> &pending_hash_ro,
                          std::list<CasRead> &pending_next_cas_ro,
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
        if (!lease_expired(it->lock)) {
          char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
          char *data_buf = AllocLocalBuffer(DataItemSize);
          pending_next_cas_ro.emplace_back(CasRead{
              .node_id = res.node_id,
              .item = res.item,
              .cas_buf = cas_buf,
              .data_buf = data_buf,
          });
          context->CompareAndSwap(
              cas_buf,
              GlobalAddress(res.node_id,
                            it->GetRemoteLockAddr(it->remote_offset)),
              it->lock, next_lease());
          context->read(data_buf, GlobalAddress(res.node_id, it->remote_offset),
                        DataItemSize);
          context->PostRequest();
        }
      }
      //   SDS_INFO("hash found");
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

bool DTX::DSLRCheckHashRW(std::vector<HashRead> &pending_hash_rw,
                          std::list<InvisibleRead> &pending_invisible_ro,
                          std::list<HashRead> &pending_next_hash_rw) {
  for (auto &res : pending_hash_rw) {
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
      if (unlikely((it->lock))) {
        return false;
      }
    } else {
      auto *local_hash_node = (HashNode *)res.buf;
      if (local_hash_node->next == nullptr) return false;
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      pending_next_hash_rw.emplace_back(HashRead{.node_id = res.node_id,
                                                 .item = res.item,
                                                 .buf = res.buf,
                                                 .meta = res.meta});
      context->read(res.buf, GlobalAddress(res.node_id, node_off),
                    sizeof(HashNode));
    }
  }
  return true;
}

bool DTX::DSLRCheckNextHashRW(std::list<InvisibleRead> &pending_invisible_ro,
                              std::list<HashRead> &pending_next_hash_rw) {
  for (auto iter = pending_next_hash_rw.begin();
       iter != pending_next_hash_rw.end();) {
    auto res = *iter;
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
      if (unlikely((it->lock))) {
        return false;
      }
    } else {
      auto *local_hash_node = (HashNode *)res.buf;
      if (local_hash_node->next == nullptr) return false;
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      pending_next_hash_rw.emplace_back(HashRead{.node_id = res.node_id,
                                                 .item = res.item,
                                                 .buf = res.buf,
                                                 .meta = res.meta});
      context->read(res.buf, GlobalAddress(res.node_id, node_off),
                    sizeof(HashNode));
    }

    iter = pending_next_hash_rw.erase(iter);
    iter++;
  }

  return true;
}

bool DTX::DSLRIssueReadOnly(std::vector<CasRead> &pending_cas_ro,
                            std::vector<HashRead> &pending_hash_ro) {
  for (auto &item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    node_id_t node_id = GetPrimaryNodeID(it->table_id);
    item.read_which_node = node_id;
    auto offset = addr_cache->Search(node_id, it->table_id, it->key);
    if (offset != NOT_FOUND) {
      it->remote_offset = offset;
      char *faa_buf = AllocLocalBuffer(sizeof(lock_t));
      char *data_buf = AllocLocalBuffer(DataItemSize);
      pending_cas_ro.emplace_back(CasRead{
          .node_id = node_id,
          .item = &item,
          .cas_buf = cas_buf,
          .data_buf = data_buf,
      });
      context->FetchAndAdd(
          faa_buf, GlobalAddress(node_id, it->GetRemoteLockAddr(offset)),
          acquire_read_lock);
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

bool DTX::DSLRCheckCasRW(std::vector<CasRead> &pending_cas_rw,
                         std::list<HashRead> &pending_next_hash_rw,
                         std::list<InsertOffRead> &pending_next_off_rw) {
  for (auto &re : pending_cas_rw) {
    if (*((lock_t *)re.cas_buf) != STATE_CLEAN) {
      return false;
    }
    auto it = re.item->item_ptr;
    auto *fetched_item = (DataItem *)(re.data_buf);
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (it->user_insert) {
        if (it->version < fetched_item->version) return false;
        old_version_for_insert.push_back(
            OldVersionForInsert{.table_id = it->table_id,
                                .key = it->key,
                                .version = fetched_item->version});
      } else {
        if (likely(fetched_item->valid)) {
          assert(fetched_item->remote_offset == it->remote_offset);
          *it = *fetched_item;
        } else {
          addr_cache->Insert(re.node_id, it->table_id, it->key, NOT_FOUND);
          return false;
        }
      }
      re.item->is_fetched = true;
    } else {
      *((lock_t *)re.cas_buf) = STATE_CLEAN;
      context->Write(re.cas_buf,
                     GlobalAddress(re.node_id, it->GetRemoteLockAddr()),
                     sizeof(lock_t));
      const HashMeta &meta = GetPrimaryHashMetaWithTableID(it->table_id);
      uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      offset_t node_off = idx * meta.node_size + meta.base_off;
      auto *local_hash_node = (HashNode *)AllocLocalBuffer(sizeof(HashNode));
      if (it->user_insert) {
        pending_next_off_rw.emplace_back(
            InsertOffRead{.node_id = re.node_id,
                          .item = re.item,
                          .buf = (char *)local_hash_node,
                          .meta = meta,
                          .node_off = node_off});
      } else {
        pending_next_hash_rw.emplace_back(
            HashRead{.node_id = re.node_id,
                     .item = re.item,
                     .buf = (char *)local_hash_node,
                     .meta = meta});
      }
      context->read(local_hash_node, GlobalAddress(re.node_id, node_off),
                    sizeof(HashNode));
      context->PostRequest();
    }
  }
  return true;
}
