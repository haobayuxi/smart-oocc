#include "dtx.h"

uint64_t DrTMLease = 400;

ALWAYS_INLINE
uint64_t next_lease() { return (get_clock_sys_time_us() + DrTMLease) << 1; }
ALWAYS_INLINE
bool DTX::lease_expired(uint64_t lock) {
  auto now = (get_clock_sys_time_us() << 1);
  if (lock > now) {
    return true;  // return true lease not expired
  }
  return false;
}

bool DTX::DrTMExeRO() {
  std::vector<CasRead> pending_cas_ro;
  std::vector<HashRead> pending_hash_ro;
  DrTMIssueReadOnly(pending_cas_ro, pending_hash_ro);
  context->Sync();
  std::list<CasRead> pending_next_cas_ro;
  std::list<HashRead> pending_next_hash_ro;
  if (!DrTMCheckDirectRO(pending_cas_ro, pending_next_cas_ro,
                         pending_next_hash_ro))
    return false;
  if (!DrTMCheckHashRO(pending_hash_ro, pending_next_cas_ro,
                       pending_next_hash_ro))
    return false;
  for (int i = 0; i < 100; i++) {
    context->Sync();
    if (!pending_next_cas_ro.empty() || !pending_next_hash_ro.empty()) {
      if (!DrTMCheckNextCasRO(pending_next_cas_ro)) return false;
      if (!DrTMCheckNextHashRO(pending_next_hash_ro, pending_next_cas_ro))
        return false;
    } else {
      // break;
      return true;
    }
  }
  return false;
}

bool DTX::DrTMExeRW() {
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
  DrTMIssueReadOnly(pending_cas_ro, pending_hash_ro);
  DrTMIssueReadWrite(pending_cas_rw, pending_hash_rw, pending_insert_off_rw);
  context->Sync();
  if (!DrTMCheckDirectRO(pending_cas_ro, pending_next_cas_ro,
                         pending_next_hash_ro))
    return false;
  if (!DrTMCheckHashRO(pending_hash_ro, pending_next_cas_ro,
                       pending_next_hash_ro))
    return false;
  if (!DrTMCheckHashRW(pending_hash_rw, pending_next_cas_rw,
                       pending_next_hash_rw))
    return false;
  if (!CheckInsertOffRW(pending_insert_off_rw, pending_invisible_ro,
                        pending_next_off_rw))
    return false;
  if (!DrTMCheckCasRW(pending_cas_rw, pending_next_cas_rw, pending_next_hash_rw,
                      pending_next_off_rw))
    return false;
  for (int i = 0; i < 100; i++) {
    context->Sync();
    if (!pending_next_cas_rw.empty() || !pending_next_cas_ro.empty() ||
        !pending_next_hash_ro.empty() || !pending_next_hash_rw.empty() ||
        !pending_next_off_rw.empty()) {
      if (!CheckInvisibleRO(pending_invisible_ro)) return false;
      if (!CheckNextHashRO(pending_next_hash_ro)) return false;
      if (!DrTMCheckNextHashRW(pending_next_cas_rw, pending_next_hash_rw))
        return false;
      if (!CheckNextOffRW(pending_invisible_ro, pending_next_off_rw))
        return false;
      if (!DrTMCheckNextCasRW(pending_next_cas_rw)) return false;
      if (!DrTMCheckNextCasRO(pending_next_cas_ro)) return false;
    } else {
      // break;
      ParallelUndoLog();
      return true;
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
  return false;
}

bool DTX::DrTMIssueReadWrite(
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
      // SDS_INFO("cache found key%ld, txid %ld", it->key, tx_id);
      locked_rw_set.emplace_back(i);
      char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
      char *data_buf = AllocLocalBuffer(DataItemSize);
      pending_cas_rw.emplace_back(CasRead{.node_id = node_id,
                                          .item = &read_write_set[i],
                                          .cas_buf = cas_buf,
                                          .data_buf = data_buf});
      context->CompareAndSwap(
          cas_buf, GlobalAddress(node_id, it->GetRemoteLockAddr(offset)),
          STATE_CLEAN, tx_id << 1 + 1);
      context->read(data_buf, GlobalAddress(node_id, offset), DataItemSize);
      context->PostRequest();
    } else {
      // not_eager_locked_rw_set.emplace_back(i);
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
        // SDS_INFO("cache not found key%ld, txid %ld", it->key, tx_id);
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

bool DTX::DrTMCheckNextCasRO(std::list<CasRead> &pending_next_cas_ro) {
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
                GlobalAddress(
                    res.node_id,
                    it->GetRemoteLockAddr(fetched_item->GetRemoteLockAddr(
                        fetched_item->remote_offset))),
                it->lock, next_lease());
            // context->PostRequest();
            context->read(
                res.data_buf,
                GlobalAddress(res.node_id, fetched_item->remote_offset),
                DataItemSize);
            context->PostRequest();
          } else {
            // SDS_INFO("key%ld, lease %ld", it->key, it->lock);
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

bool DTX::DrTMCheckDirectRO(std::vector<CasRead> &pending_cas_ro,
                            std::list<CasRead> &pending_next_cas_ro,
                            std::list<HashRead> &pending_next_hash_ro) {
  for (auto &res : pending_cas_ro) {
    auto *it = res.item->item_ptr.get();
    auto *fetched_item = (DataItem *)res.data_buf;
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (likely(fetched_item->valid)) {
        *it = *fetched_item;
        res.item->is_fetched = true;
        if (it->lock % 2 == 1) {
          // write locked
          //   SDS_INFO("write locked");
          return false;
        } else {
          if (!lease_expired(it->lock)) {
            // SDS_INFO("txid =%ld,key=%ld,nextlease=%ld,direct lease expired",
            //          tx_id, it->key, next_lease());
            // retry
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
                GlobalAddress(res.node_id, it->GetRemoteLockAddr(
                                               fetched_item->remote_offset)),
                it->lock, next_lease());
            // context->PostRequest();
            context->read(
                data_buf,
                GlobalAddress(res.node_id, fetched_item->remote_offset),
                DataItemSize);
            context->PostRequest();
          } else {
            // SDS_INFO("txid =%ld,key=%ld,lease not expired %ld", tx_id,
            // it->key,
            //          it->lock);
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
      context->PostRequest();
    }
  }
  return true;
}

bool DTX::DrTMCheckHashRO(std::vector<HashRead> &pending_hash_ro,
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
          // context->PostRequest();
          context->read(data_buf, GlobalAddress(res.node_id, it->remote_offset),
                        DataItemSize);
          context->PostRequest();
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
      context->PostRequest();
    }
  }
  return true;
}

bool DTX::DrTMCheckHashRW(std::vector<HashRead> &pending_hash_rw,
                          std::list<CasRead> &pending_next_cas_rw,
                          std::list<HashRead> &pending_next_hash_rw) {
  for (auto &res : pending_hash_rw) {
    auto *local_hash_node = (HashNode *)res.buf;
    auto *it = res.item->item_ptr.get();
    bool find = false;
    for (auto &item : local_hash_node->data_items) {
      if (item.valid && item.key == it->key && item.table_id == it->table_id) {
        *it = item;
        // SDS_INFO("insert into cache key%ld", it->key);
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
          // get write lock and data
          char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
          char *data_buf = AllocLocalBuffer(DataItemSize);
          pending_next_cas_rw.emplace_back(CasRead{.node_id = res.node_id,
                                                   .item = res.item,
                                                   .cas_buf = cas_buf,
                                                   .data_buf = data_buf});
          context->CompareAndSwap(
              cas_buf,
              GlobalAddress(res.node_id,
                            it->GetRemoteLockAddr(it->remote_offset)),
              it->lock, tx_id << 1 + 1);
          context->read(data_buf, GlobalAddress(res.node_id, it->remote_offset),
                        DataItemSize);
          context->PostRequest();
        } else {
          return false;
        }
      }
    } else {
      return false;
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
      context->PostRequest();
    }
  }
  return true;
}

bool DTX::DrTMCheckNextHashRW(std::list<CasRead> &pending_next_cas_rw,
                              std::list<HashRead> &pending_next_hash_rw) {
  for (auto iter = pending_next_hash_rw.begin();
       iter != pending_next_hash_rw.end(); iter++) {
    auto res = *iter;
    auto *local_hash_node = (HashNode *)res.buf;
    auto *it = res.item->item_ptr.get();
    bool find = false;
    for (auto &item : local_hash_node->data_items) {
      if (item.valid && item.key == it->key && item.table_id == it->table_id) {
        *it = item;
        // SDS_INFO("insert into cache key%ld", it->key);
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
      } else {
        char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
        char *data_buf = AllocLocalBuffer(DataItemSize);
        pending_next_cas_rw.emplace_back(CasRead{.node_id = res.node_id,
                                                 .item = res.item,
                                                 .cas_buf = cas_buf,
                                                 .data_buf = data_buf});
        context->CompareAndSwap(
            cas_buf,
            GlobalAddress(res.node_id,
                          it->GetRemoteLockAddr(it->remote_offset)),
            STATE_CLEAN, tx_id << 1 + 1);
        context->read(data_buf, GlobalAddress(res.node_id, it->remote_offset),
                      DataItemSize);
        context->PostRequest();
      }
    } else {
      // return false;
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
      context->PostRequest();
    }

    iter = pending_next_hash_rw.erase(iter);
  }

  return true;
}

bool DTX::DrTMIssueReadOnly(std::vector<CasRead> &pending_cas_ro,
                            std::vector<HashRead> &pending_hash_ro) {
  uint64_t read_lease = (start_time + DrTMLease) << 1;
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
          .node_id = node_id,
          .item = &item,
          .cas_buf = cas_buf,
          .data_buf = data_buf,
      });
      // SDS_INFO("txid =%ld,key=%ld,lease=%ld", tx_id, it->key, read_lease);
      context->CompareAndSwap(
          cas_buf, GlobalAddress(node_id, it->GetRemoteLockAddr(offset)), 0,
          read_lease);
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

bool DTX::DrTMCheckCasRW(std::vector<CasRead> &pending_cas_rw,
                         std::list<CasRead> &pending_next_cas_rw,
                         std::list<HashRead> &pending_next_hash_rw,
                         std::list<InsertOffRead> &pending_next_off_rw) {
  for (auto &re : pending_cas_rw) {
    auto lock = *((lock_t *)re.cas_buf);

    auto *it = re.item->item_ptr.get();
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
          if (lock != 0) {
            // check lease
            if (lock % 2 == 1) {
              // write locked
              return false;
            }
            if (lease_expired(lock)) {
              // lease not expired , abort
              return false;
            } else {
              // cas to get write lock
              char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
              char *data_buf = AllocLocalBuffer(DataItemSize);
              pending_next_cas_rw.emplace_back(CasRead{
                  .node_id = re.node_id,
                  .item = re.item,
                  .cas_buf = cas_buf,
                  .data_buf = data_buf,
              });
              // SDS_INFO("get next lock key%ld", fetched_item->key);
              auto offset =
                  fetched_item->GetRemoteLockAddr(fetched_item->remote_offset);
              context->CompareAndSwap(cas_buf,
                                      GlobalAddress(re.node_id, offset), lock,
                                      tx_id << 1 + 1);
              context->read(
                  data_buf,
                  GlobalAddress(re.node_id, fetched_item->remote_offset),
                  DataItemSize);
              context->PostRequest();
              continue;
            }
          }
          *it = *fetched_item;
        } else {
          addr_cache->Insert(re.node_id, it->table_id, it->key, NOT_FOUND);
          return false;
        }
      }
      re.item->is_fetched = true;
    } else {
      addr_cache->Insert(re.node_id, it->table_id, it->key, NOT_FOUND);
      // *((lock_t *)re.cas_buf) = STATE_CLEAN;
      // context->Write(re.cas_buf,
      //                GlobalAddress(re.node_id, it->GetRemoteLockAddr()),
      //                sizeof(lock_t));
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

bool DTX::DrTMCheckNextCasRW(std::list<CasRead> &pending_next_cas_rw) {
  for (auto iter = pending_next_cas_rw.begin();
       iter != pending_next_cas_rw.end(); iter++) {
    auto res = *iter;

    auto *it = res.item->item_ptr.get();
    auto *fetched_item = (DataItem *)(res.data_buf);
    auto lock = *((lock_t *)res.cas_buf);
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
          // SDS_INFO("found key%ld", it->key);
          if (fetched_item->lock == (tx_id << 1 + 1)) {
            *it = *fetched_item;
            res.item->is_fetched = true;

            iter = pending_next_cas_rw.erase(iter);
          } else if (lock % 2 == 1) {
            // write locked
            return false;
          } else if (lease_expired(lock)) {
            // lease not expired , abort
            return false;
          } else {
            // cas to get write lock
            auto offset =
                fetched_item->GetRemoteLockAddr(fetched_item->remote_offset);
            context->CompareAndSwap(res.cas_buf,
                                    GlobalAddress(res.node_id, offset), lock,
                                    tx_id << 1 + 1);
            context->read(
                res.data_buf,
                GlobalAddress(res.node_id, fetched_item->remote_offset),
                DataItemSize);
            context->PostRequest();
          }
        } else {
          addr_cache->Insert(res.node_id, it->table_id, it->key, NOT_FOUND);
          return false;
        }
      }
    } else {
      addr_cache->Insert(res.node_id, it->table_id, it->key, NOT_FOUND);
      return false;
    }
  }
  return true;
}

bool DTX::DrTMCheckNextHashRO(std::list<HashRead> &pending_next_hash_ro,
                              std::list<CasRead> &pending_next_cas_ro) {
  for (auto iter = pending_next_hash_ro.begin();
       iter != pending_next_hash_ro.end();) {
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
      if (!item.valid) {
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
          // context->PostRequest();
          context->read(data_buf, GlobalAddress(res.node_id, it->remote_offset),
                        DataItemSize);
          context->PostRequest();
        }
      }

      iter = pending_next_hash_ro.erase(iter);
    } else {
      // return false;
      if (local_hash_node->next == nullptr) return false;
      auto node_off = (uint64_t)local_hash_node->next - res.meta.data_ptr +
                      res.meta.base_off;
      context->read(res.buf, GlobalAddress(res.node_id, node_off),
                    sizeof(HashNode));
      context->PostRequest();
      iter++;
    }
  }
  return true;
}