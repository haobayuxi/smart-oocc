
#include <iostream>

#include "dtx.h"

using namespace std;

const uint64_t COUNT_MAX = 32768;
const int max_op = 100;

enum DSLR_CHECK_LOCK : int {
  SUCCESS = 0,
  WAIT = 1,
  BACKOFF = 2,
};

// | max x| max s| n x| n s|  lock representation
// reset read lock |prev maxx|count max| prev maxx| count max
// reset write lock |count max|prev maxs|count max| prev maxs
uint64_t reset_write_lock(uint64_t maxs) {
  uint64_t lock = COUNT_MAX << 16;
  lock += maxs;
  uint64_t result = lock << 32;
  return result + lock;
}
// 01011100111101111000000000000000
// 01011100111101111000000000000000
uint64_t reset_read_lock(uint64_t maxx) {
  uint64_t lock = maxx << 16;
  lock += COUNT_MAX;
  uint64_t result = lock << 32;
  return result + lock;
}

#define nx_mask 0x00000000FFFF0000
#define ns_mask 0x000000000000FFFF
#define max_x_mask 0xFFFF000000000000
#define max_s_mask 0x0000FFFF00000000

#define acquire_read_lock 0x0000000100000000
#define acquire_write_lock 0x0001000000000000
#define release_read_lock 0x0000000000000001
#define release_write_lock 0x0000000000010000

#define max_s_minus1 0xFFFFFFFF00000000
#define max_x_minus1 0xFFFF000000000000

uint64_t get_max_x(uint64_t lock) {
  auto maxx = lock & max_x_mask;
  return maxx >> 48;
}

uint64_t get_max_s(uint64_t lock) {
  auto maxs = lock & max_s_mask;
  return maxs >> 32;
}

uint64_t get_nx(uint64_t lock) {
  auto nx = lock & nx_mask;
  return nx >> 16;
}

uint64_t get_ns(uint64_t lock) { return lock & ns_mask; }

bool DTX::DSLRExeRO() {
  bool result = true;
  std::vector<CasRead> pending_cas_ro;
  std::vector<HashRead> pending_hash_ro;
  DSLRIssueReadOnly(pending_cas_ro, pending_hash_ro);
  context->Sync();
  std::list<CasRead> pending_next_cas_ro;
  std::list<DirectRead> pending_next_direct_ro;
  std::list<InvisibleRead> pending_invisible_ro;
  std::list<HashRead> pending_next_hash_ro;
  if (!DSLRCheckCasRO(pending_cas_ro, pending_next_direct_ro,
                      pending_next_hash_ro))
    return false;
  if (!DSLRCheckHashRO(pending_hash_ro, pending_next_cas_ro,
                       pending_next_hash_ro))
    return false;
  for (int i = 0; i < 50; i++) {
    context->Sync();
    if (!pending_next_direct_ro.empty() || !pending_next_cas_ro.empty() ||
        !pending_next_hash_ro.empty()) {
      if (!DSLRCheckNextCasRO(pending_next_cas_ro, pending_next_direct_ro))
        return false;
      if (!DSLRCheckNextHashRO(pending_next_cas_ro, pending_next_hash_ro))
        return false;
      if (!DSLRCheckDirectRO(pending_next_direct_ro)) return false;

    } else {
      // break;
      return true;
    }
  }

  return false;
}

bool DTX::DSLRExeRW() {
  is_ro_tx = false;
  context->Sync();
  std::vector<CasRead> pending_cas_ro;
  std::vector<HashRead> pending_hash_ro;
  std::vector<CasRead> pending_cas_rw;
  std::vector<HashRead> pending_hash_rw;
  std::vector<InsertOffRead> pending_insert_off_rw;
  std::list<CasRead> pending_next_cas_ro;
  std::list<CasRead> pending_next_cas_rw;
  std::list<DirectRead> pending_next_direct_ro;
  std::list<DirectRead> pending_next_direct_rw;
  std::list<InvisibleRead> pending_invisible_ro;
  std::list<HashRead> pending_next_hash_ro;
  std::list<HashRead> pending_next_hash_rw;
  std::list<InsertOffRead> pending_next_off_rw;
  DSLRIssueReadOnly(pending_cas_ro, pending_hash_ro);
  DSLRIssueReadWrite(pending_cas_rw, pending_hash_rw, pending_insert_off_rw);
  context->Sync();
  bool result = true;
  if (!DSLRCheckCasRO(pending_cas_ro, pending_next_direct_ro,
                      pending_next_hash_ro))
    result = false;
  if (!DSLRCheckCasRW(pending_cas_rw, pending_next_hash_rw,
                      pending_next_direct_rw, pending_next_off_rw))
    result = false;
  if (!result) return false;
  if (!DSLRCheckHashRO(pending_hash_ro, pending_next_cas_ro,
                       pending_next_hash_ro))
    return false;
  if (!DSLRCheckHashRW(pending_hash_rw, pending_next_cas_rw,
                       pending_next_hash_rw))
    return false;
  if (!CheckInsertOffRW(pending_insert_off_rw, pending_invisible_ro,
                        pending_next_off_rw))
    return false;
  for (int i = 0; i < 50; i++) {
    context->Sync();
    if (!pending_next_direct_ro.empty() || !pending_next_direct_rw.empty() ||
        !pending_next_hash_ro.empty() || !pending_next_hash_rw.empty() ||
        !pending_next_off_rw.empty() || !pending_next_cas_rw.empty() ||
        !pending_next_cas_ro.empty()) {
      if (!DSLRCheckNextCasRO(pending_next_cas_ro, pending_next_direct_ro))
        result = false;
      if (!DSLRCheckNextCasRW(pending_next_cas_rw, pending_next_direct_rw))
        result = false;
      if (!result) return false;
      // context->Sync();
      if (!DSLRCheckNextHashRO(pending_next_cas_ro, pending_next_hash_ro))
        return false;
      if (!DSLRCheckNextHashRW(pending_next_cas_rw, pending_next_hash_rw))
        return false;
      // context->Sync();
      if (!DSLRCheckDirectRO(pending_next_direct_ro)) return false;
      if (!DSLRCheckDirectRW(pending_next_direct_rw)) return false;

      // context->Sync();
      if (!CheckNextOffRW(pending_invisible_ro, pending_next_off_rw))
        return false;
    } else {
      // break;

      ParallelUndoLog();
      return true;
    }
  }

  return false;
}

bool DTX::DSLRIssueReadWrite(
    std::vector<CasRead> &pending_cas_rw,
    std::vector<HashRead> &pending_hash_rw,
    std::vector<InsertOffRead> &pending_insert_off_rw) {
  // SDS_INFO("issue read write %ld", tx_id);
  for (size_t i = 0; i < read_write_set.size(); i++) {
    if (read_write_set[i].is_fetched) continue;
    auto it = read_write_set[i].item_ptr;
    auto node_id = GetPrimaryNodeID(it->table_id);
    read_write_set[i].read_which_node = node_id;
    auto offset = addr_cache->Search(node_id, it->table_id, it->key);
    if (offset != NOT_FOUND) {
      it->remote_offset = offset;
      locked_rw_set.emplace_back(i);
      char *faa_buf = AllocLocalBuffer(sizeof(lock_t));
      char *data_buf = AllocLocalBuffer(DataItemSize);
      pending_cas_rw.emplace_back(CasRead{.node_id = node_id,
                                          .item = &read_write_set[i],
                                          .cas_buf = faa_buf,
                                          .data_buf = data_buf});
      context->FetchAndAdd(
          faa_buf, GlobalAddress(node_id, it->GetRemoteLockAddr(offset)),
          acquire_write_lock);
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

bool DTX::DSLRCheckNextCasRO(std::list<CasRead> &pending_next_cas_ro,
                             std::list<DirectRead> pending_next_direct_ro) {
  bool result = true;
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
        auto lock = *(lock_t *)res.cas_buf;
        uint64_t maxs = get_max_s(lock);
        uint64_t maxx = get_max_x(lock);
        res.item->prev_maxs = maxs;
        res.item->prev_maxx = maxx;
        if (maxs >= COUNT_MAX || maxx >= COUNT_MAX) {
          // backoff
          result = false;
        }
        if (maxs == COUNT_MAX - 1) {
          auto reset_lock = reset_read_lock(maxx);
          char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
          memset(cas_buf, 0, sizeof(lock_t));
          reset.emplace_back(ResetLock{
              .offset = it->GetRemoteLockAddr(),
              .lock = reset_lock,
              .cas_buf = cas_buf,
          });
        }
        if (maxx != get_nx(lock)) {
          char *data_buf = AllocLocalBuffer(DataItemSize);
          pending_next_direct_ro.emplace_back(DirectRead{
              .node_id = res.node_id,
              .item = res.item,
              .buf = data_buf,
              .prev_maxx = maxx,
          });
          context->read(data_buf,
                        GlobalAddress(res.node_id, fetched_item->remote_offset),
                        DataItemSize);
          context->PostRequest();
        }

        iter = pending_next_cas_ro.erase(iter);
      } else {
        result = false;
      }
    } else {
      result = false;
    }
  }

  return result;
}

bool DTX::DSLRCheckNextCasRW(std::list<CasRead> &pending_next_cas_rw,
                             std::list<DirectRead> pending_next_direct_rw) {
  bool result = true;
  for (auto iter = pending_next_cas_rw.begin();
       iter != pending_next_cas_rw.end(); iter++) {
    auto res = *iter;
    auto *it = res.item->item_ptr.get();
    auto *fetched_item = (DataItem *)res.data_buf;
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (likely(fetched_item->valid)) {
        *it = *fetched_item;
        res.item->is_fetched = true;
        auto lock = *(lock_t *)res.cas_buf;
        uint64_t maxs = get_max_s(lock);
        uint64_t maxx = get_max_x(lock);
        res.item->prev_maxs = maxs;
        res.item->prev_maxx = maxx;
        if (maxs >= COUNT_MAX || maxx >= COUNT_MAX) {
          result = false;
        }
        if (maxx == COUNT_MAX - 1) {
          auto reset_lock = reset_write_lock(maxs);
          char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
          memset(cas_buf, 0, sizeof(lock_t));
          // SDS_INFO("maxx %ld", reset_lock);
          reset.emplace_back(ResetLock{
              .offset = it->GetRemoteLockAddr(),
              .lock = reset_lock,
              .cas_buf = cas_buf,
          });
        }
        if (maxs == get_ns(lock) && maxx == get_nx(lock)) {
        } else {
          char *data_buf = AllocLocalBuffer(DataItemSize);
          pending_next_direct_rw.emplace_back(DirectRead{
              .node_id = res.node_id,
              .item = res.item,
              .buf = data_buf,
              .prev_maxs = maxs,
              .prev_maxx = maxx,
          });
          context->read(data_buf,
                        GlobalAddress(res.node_id, fetched_item->remote_offset),
                        DataItemSize);
          context->PostRequest();
        }

        iter = pending_next_cas_rw.erase(iter);
      } else {
        result = false;
      }
    } else {
      result = false;
    }
  }

  return result;
}

bool DTX::DSLRCheckCasRO(std::vector<CasRead> &pending_cas_ro,
                         std::list<DirectRead> &pending_next_direct_ro,
                         std::list<HashRead> &pending_next_hash_ro) {
  bool result = true;
  for (auto &res : pending_cas_ro) {
    auto *it = res.item->item_ptr.get();
    auto *fetched_item = (DataItem *)res.data_buf;
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (likely(fetched_item->valid)) {
        *it = *fetched_item;
        res.item->is_fetched = true;
        uint64_t lock = *(uint64_t *)res.cas_buf;
        uint64_t maxs = get_max_s(lock);
        uint64_t maxx = get_max_x(lock);
        res.item->prev_maxs = maxs;
        res.item->prev_maxx = maxx;
        if (maxs >= COUNT_MAX || maxx >= COUNT_MAX) {
          result = false;
        }
        if (maxs == COUNT_MAX - 1) {
          // SDS_INFO("%ld", maxx);
          auto reset_lock = reset_read_lock(maxx);
          char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
          memset(cas_buf, 0, sizeof(lock_t));

          // SDS_INFO("%ld", reset_lock);
          reset.emplace_back(ResetLock{
              .offset = it->remote_offset,
              .lock = reset_lock,
              .cas_buf = cas_buf,
          });
        }
        if (get_max_x(it->lock) != get_nx(it->lock)) {
          char *data_buf = AllocLocalBuffer(DataItemSize);
          pending_next_direct_ro.emplace_back(DirectRead{
              .node_id = res.node_id,
              .item = res.item,
              .buf = data_buf,
              .prev_maxx = maxx,
          });
          context->read(data_buf,
                        GlobalAddress(res.node_id, fetched_item->remote_offset),
                        DataItemSize);
          context->PostRequest();
        }

      } else {
        res.item->is_fetched = true;
        addr_cache->Insert(res.node_id, it->table_id, it->key, NOT_FOUND);
        result = false;
      }
    } else {
      res.item->is_fetched = true;
      addr_cache->Insert(res.node_id, it->table_id, it->key, NOT_FOUND);
      result = false;
      // node_id_t remote_node_id = GetPrimaryNodeID(it->table_id);
      // const HashMeta &meta = GetPrimaryHashMetaWithTableID(it->table_id);
      // uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      // offset_t node_off = idx * meta.node_size + meta.base_off;
      // auto *local_hash_node = (HashNode *)AllocLocalBuffer(sizeof(HashNode));
      // pending_next_hash_ro.emplace_back(HashRead{.node_id = remote_node_id,
      //                                            .item = res.item,
      //                                            .buf = (char
      //                                            *)local_hash_node, .meta =
      //                                            meta});
      // context->read((char *)local_hash_node,
      //               GlobalAddress(remote_node_id, node_off),
      //               sizeof(HashNode));
    }
  }
  return result;
}

bool DTX::DSLRCheckDirectRO(std::list<DirectRead> &pending_next_direct_ro) {
  for (auto iter = pending_next_direct_ro.begin();
       iter != pending_next_direct_ro.end(); iter++) {
    auto res = *iter;
    auto *fetched_item = (DataItem *)res.buf;

    auto *it = res.item->item_ptr.get();
    *it = *fetched_item;
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (likely(fetched_item->valid)) {
        if (res.prev_maxx != get_nx(it->lock)) {
          // char *data_buf = AllocLocalBuffer(DataItemSize);
          pending_next_direct_ro.emplace_back(DirectRead{
              .node_id = res.node_id,
              .item = res.item,
              .buf = res.buf,
              .prev_maxx = res.prev_maxx,
          });
          auto ret = context->read(
              res.buf, GlobalAddress(res.node_id, fetched_item->remote_offset),
              DataItemSize);
          if (ret != 0) {
            return false;
          }
          context->PostRequest();
        }
        iter = pending_next_direct_ro.erase(iter);
      } else {
        return false;
      }
    }
  }
  return true;
}

bool DTX::DSLRCheckDirectRW(std::list<DirectRead> &pending_next_direct_rw) {
  for (auto iter = pending_next_direct_rw.begin();
       iter != pending_next_direct_rw.end(); iter++) {
    auto res = *iter;
    auto *fetched_item = (DataItem *)res.buf;
    auto *it = res.item->item_ptr.get();
    *it = *fetched_item;
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (likely(fetched_item->valid)) {
        if (get_ns(it->lock) != res.prev_maxs ||
            get_nx(it->lock) != res.prev_maxx) {
          // read locked
          // char *data_buf = AllocLocalBuffer(DataItemSize);
          pending_next_direct_rw.emplace_back(DirectRead{
              .node_id = res.node_id,
              .item = res.item,
              .buf = res.buf,
              .prev_maxs = res.prev_maxs,
          });
          auto offset = addr_cache->Search(res.node_id, it->table_id, it->key);
          auto ret = context->read(res.buf, GlobalAddress(res.node_id, offset),
                                   DataItemSize);
          if (ret != 0) {
            return false;
          }
          context->PostRequest();
        }
        iter = pending_next_direct_rw.erase(iter);
      } else {
        return false;
      }
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
      // retry to get read lock
      char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
      char *data_buf = AllocLocalBuffer(DataItemSize);
      pending_next_cas_ro.emplace_back(CasRead{
          .node_id = res.node_id,
          .item = res.item,
          .cas_buf = cas_buf,
          .data_buf = data_buf,
      });
      context->FetchAndAdd(
          cas_buf,
          GlobalAddress(res.node_id, it->GetRemoteLockAddr(it->remote_offset)),
          acquire_read_lock);
      context->read(data_buf, GlobalAddress(res.node_id, it->remote_offset),
                    DataItemSize);
      context->PostRequest();
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

bool DTX::DSLRCheckNextHashRO(std::list<CasRead> &pending_next_cas_ro,
                              std::list<HashRead> &pending_next_hash_ro) {
  for (auto iter = pending_next_hash_ro.begin();
       iter != pending_next_hash_ro.end(); iter++) {
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
      // retry to get read lock
      char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
      char *data_buf = AllocLocalBuffer(DataItemSize);
      pending_next_cas_ro.emplace_back(CasRead{
          .node_id = res.node_id,
          .item = res.item,
          .cas_buf = cas_buf,
          .data_buf = data_buf,
      });
      context->FetchAndAdd(
          cas_buf,
          GlobalAddress(res.node_id, it->GetRemoteLockAddr(it->remote_offset)),
          acquire_read_lock);
      context->read(data_buf, GlobalAddress(res.node_id, it->remote_offset),
                    DataItemSize);
      context->PostRequest();

    } else {
      // return false;
      auto *local_hash_node = (HashNode *)res.buf;
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
    iter = pending_next_hash_ro.erase(iter);
  }

  return true;
}

bool DTX::DSLRCheckHashRW(std::vector<HashRead> &pending_hash_rw,
                          std::list<CasRead> &pending_next_cas_rw,
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
      // retry to get write lock
      char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
      char *data_buf = AllocLocalBuffer(DataItemSize);
      pending_next_cas_rw.emplace_back(CasRead{
          .node_id = res.node_id,
          .item = res.item,
          .cas_buf = cas_buf,
          .data_buf = data_buf,
      });
      context->FetchAndAdd(
          cas_buf,
          GlobalAddress(res.node_id, it->GetRemoteLockAddr(it->remote_offset)),
          acquire_write_lock);
      context->read(data_buf, GlobalAddress(res.node_id, it->remote_offset),
                    DataItemSize);
      context->PostRequest();
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

bool DTX::DSLRCheckNextHashRW(std::list<CasRead> &pending_next_cas_rw,
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
      // retry to get write lock
      char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
      char *data_buf = AllocLocalBuffer(DataItemSize);
      pending_next_cas_rw.emplace_back(CasRead{
          .node_id = res.node_id,
          .item = res.item,
          .cas_buf = cas_buf,
          .data_buf = data_buf,
      });
      context->FetchAndAdd(
          cas_buf,
          GlobalAddress(res.node_id, it->GetRemoteLockAddr(it->remote_offset)),
          acquire_write_lock);
      context->read(data_buf, GlobalAddress(res.node_id, it->remote_offset),
                    DataItemSize);
      context->PostRequest();
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
    }

    iter = pending_next_hash_rw.erase(iter);
    // iter++;
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
          .cas_buf = faa_buf,
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
                         std::list<DirectRead> &pending_next_direct_rw,
                         std::list<InsertOffRead> &pending_next_off_rw) {
  bool result = true;
  for (auto &re : pending_cas_rw) {
    auto it = re.item->item_ptr;
    auto *fetched_item = (DataItem *)(re.data_buf);
    if (likely(fetched_item->key == it->key &&
               fetched_item->table_id == it->table_id)) {
      if (it->user_insert) {
        // if (it->version < fetched_item->version) return false;
        old_version_for_insert.push_back(
            OldVersionForInsert{.table_id = it->table_id,
                                .key = it->key,
                                .version = fetched_item->version});
      } else {
        if (likely(fetched_item->valid)) {
          assert(fetched_item->remote_offset == it->remote_offset);
          //  {
          //   result = false;
          // }
          *it = *fetched_item;
          re.item->is_fetched = true;
          uint64_t lock = *(lock_t *)re.cas_buf;

          uint64_t maxs = get_max_s(lock);
          uint64_t maxx = get_max_x(lock);
          re.item->prev_maxs = maxs;
          re.item->prev_maxx = maxx;
          if (maxs >= COUNT_MAX || maxx >= COUNT_MAX) {
            result = false;
          }
          if (maxx == COUNT_MAX - 1) {
            // for (int i = 63; i >= 0; i--) {
            //   cout << ((lock >> i) & 1);
            // }
            // cout << endl;
            auto reset_lock = reset_write_lock(maxs);
            char *cas_buf = AllocLocalBuffer(sizeof(lock_t));
            memset(cas_buf, 0, sizeof(lock_t));
            // SDS_INFO("maxs = %ld, %ld", maxs, reset_lock);
            reset.emplace_back(ResetLock{
                .offset = it->GetRemoteLockAddr(),
                .lock = reset_lock,
                .cas_buf = cas_buf,
            });
          }
          if (maxs == get_ns(it->lock) && maxx == get_nx(it->lock)) {
          } else {
            char *data_buf = AllocLocalBuffer(DataItemSize);
            pending_next_direct_rw.emplace_back(DirectRead{
                .node_id = re.node_id,
                .item = re.item,
                .buf = data_buf,
                .prev_maxs = maxs,
                .prev_maxx = maxx,
            });
            context->read(
                data_buf,
                GlobalAddress(re.node_id, fetched_item->remote_offset),
                DataItemSize);
            context->PostRequest();
          }
          // SDS_INFO("nslock = %ld, maxs %ld", get_ns(it->lock), maxs);
          // if (!check_write_lock(it->lock)) {
          //   char *data_buf = AllocLocalBuffer(DataItemSize);
          //   pending_next_direct_rw.emplace_back(DirectRead{
          //       .node_id = re.node_id,
          //       .item = re.item,
          //       .buf = data_buf,
          //   });
          //   context->read(data_buf,
          //                 GlobalAddress(re.node_id, it->remote_offset),
          //                 DataItemSize);
          //   context->PostRequest();
          // }
        } else {
          addr_cache->Insert(re.node_id, it->table_id, it->key, NOT_FOUND);
          result = false;
        }
      }
      re.item->is_fetched = true;
    } else {
      addr_cache->Insert(re.node_id, it->table_id, it->key, NOT_FOUND);
      result = false;
      // *((lock_t *)re.cas_buf) = STATE_CLEAN;
      // context->Write(re.cas_buf,
      //                GlobalAddress(re.node_id, it->GetRemoteLockAddr()),
      //                sizeof(lock_t));
      // const HashMeta &meta = GetPrimaryHashMetaWithTableID(it->table_id);
      // uint64_t idx = MurmurHash64A(it->key, 0xdeadbeef) % meta.bucket_num;
      // offset_t node_off = idx * meta.node_size + meta.base_off;
      // auto *local_hash_node = (HashNode *)AllocLocalBuffer(sizeof(HashNode));
      // if (it->user_insert) {
      //   pending_next_off_rw.emplace_back(
      //       InsertOffRead{.node_id = re.node_id,
      //                     .item = re.item,
      //                     .buf = (char *)local_hash_node,
      //                     .meta = meta,
      //                     .node_off = node_off});
      // } else {
      //   pending_next_hash_rw.emplace_back(
      //       HashRead{.node_id = re.node_id,
      //                .item = re.item,
      //                .buf = (char *)local_hash_node,
      //                .meta = meta});
      // }
      // context->read(local_hash_node, GlobalAddress(re.node_id, node_off),
      //               sizeof(HashNode));
      // context->PostRequest();
    }
  }
  return result;
}

bool DTX::DSLRCommit() {
  context->Sync();
  int index = 0;
  while (index < read_only_set.size()) {
    for (int j = 0; j < max_op; j++) {
      auto item = read_only_set[index];
      char *faa_buf = AllocLocalBuffer(sizeof(lock_t));
      auto *it = item.item_ptr.get();
      node_id_t node_id = item.read_which_node;

      context->FetchAndAdd(faa_buf,
                           GlobalAddress(node_id, it->GetRemoteLockAddr()),
                           release_read_lock);
      context->PostRequest();
      index++;
      if (index == read_only_set.size()) {
        break;
      }
    }
    context->Sync();
  }

  for (auto &set_it : read_write_set) {
    char *data_buf = AllocLocalBuffer(DataItemSize - sizeof(lock_t));
    auto it = set_it.item_ptr;
    if (!it->user_insert) {
      it->version++;
    }

    memcpy(data_buf, (char *)it.get() + sizeof(lock_t),
           DataItemSize - sizeof(lock_t));
    // SDS_INFO("table id = %d", it->table_id);
    node_id_t node_id = set_it.read_which_node;

    context->Write(data_buf,
                   GlobalAddress(node_id, it->remote_offset + sizeof(lock_t)),
                   DataItemSize - sizeof(lock_t));
    context->PostRequest();
  }
  context->Sync();
  for (auto &set_it : read_write_set) {
    char *faa_buf = AllocLocalBuffer(sizeof(lock_t));
    auto it = set_it.item_ptr;

    node_id_t node_id = set_it.read_which_node;

    context->FetchAndAdd(faa_buf,
                         GlobalAddress(node_id, it->GetRemoteLockAddr()),
                         release_write_lock);
    context->PostRequest();
  }
  context->Sync();

  while (reset.size() > 0) {
    context->Sync();
    CheckReset();
  }
  return true;
}

bool DTX::CheckReset() {
  for (auto iter = reset.begin(); iter != reset.end(); iter++) {
    auto res = *iter;

    if (*((uint64_t *)res.cas_buf) != res.lock) {
      auto cas = *(uint64_t *)res.cas_buf;
      // SDS_INFO("%ld, %ld", *(uint64_t *)res.cas_buf, res.lock);
      // for (int i = 63; i >= 0; i--) {
      //   cout << ((cas >> i) & 1);
      // }
      // cout << endl;
      // for (int i = 63; i >= 0; i--) {
      //   cout << ((res.lock >> i) & 1);
      // }
      // cout << endl;
      // assert(false);
      context->CompareAndSwap(res.cas_buf, GlobalAddress(0, res.offset),
                              res.lock, 0);
      context->PostRequest();
    } else {
      iter = reset.erase(iter);
    }
  }
  return true;
}

bool DTX::DSLRAbort() {
  // release read lock
  context->Sync();
  int index = 0;
  while (index < read_only_set.size()) {
    for (int j = 0; j < max_op; j++) {
      auto item = read_only_set[index];
      char *faa_buf = AllocLocalBuffer(sizeof(lock_t));
      auto *it = item.item_ptr.get();
      node_id_t node_id = item.read_which_node;
      // check backoff
      auto lock = it->lock;
      if (item.prev_maxs >= COUNT_MAX || item.prev_maxx >= COUNT_MAX) {
        context->FetchAndAdd(faa_buf,
                             GlobalAddress(node_id, it->GetRemoteLockAddr()),
                             max_s_minus1);
      } else {
        context->FetchAndAdd(faa_buf,
                             GlobalAddress(node_id, it->GetRemoteLockAddr()),
                             release_read_lock);
      }

      context->PostRequest();
      index++;
      if (index == read_only_set.size()) {
        break;
      }
    }
    context->Sync();
  }

  // release write lock
  for (auto &item : read_write_set) {
    char *faa_buf = AllocLocalBuffer(sizeof(lock_t));
    auto it = item.item_ptr;

    node_id_t node_id = item.read_which_node;
    // check backoff
    auto lock = it->lock;
    auto maxx = get_max_x(lock);
    if (item.prev_maxs >= COUNT_MAX || item.prev_maxx >= COUNT_MAX) {
      context->FetchAndAdd(faa_buf,
                           GlobalAddress(node_id, it->GetRemoteLockAddr()),
                           max_x_minus1);
    } else {
      context->FetchAndAdd(faa_buf,
                           GlobalAddress(node_id, it->GetRemoteLockAddr()),
                           release_write_lock);
    }

    context->PostRequest();
  }
  context->Sync();
  while (reset.size() > 0) {
    // SDS_INFO("abort %d %ld", reset.size(), tx_id);
    context->Sync();
    CheckReset();
  }
  return true;
}