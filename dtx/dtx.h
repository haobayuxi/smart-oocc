// Some contents of this file are derived from FORD
// https://github.com/minghust/FORD

#pragma once

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <list>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "addr_cache.h"
#include "common.h"
#include "manager.h"
#include "memstore.h"
#include "smart/task_throttler.h"

using namespace sds;
using HashNode = HashStore::HashNode;

enum DTX_SYS : int {
  OOCC = 0,
  DrTMH = 1,
  DSLR = 2,
  OCC = 3,
};
struct DataSetItem {
  DataItemPtr item_ptr;
  bool is_fetched;
  bool is_logged;
  node_id_t read_which_node;
  uint64_t lease;
};

struct OldVersionForInsert {
  table_id_t table_id;
  itemkey_t key;
  version_t version;
};

struct DirectRead {
  node_id_t node_id;
  DataSetItem *item;
  char *buf;
};

struct HashRead {
  node_id_t node_id;
  DataSetItem *item;
  char *buf;
  const HashMeta meta;
};

struct CasRead {
  node_id_t node_id;
  DataSetItem *item;
  char *cas_buf;
  char *data_buf;
};

struct InsertOffRead {
  node_id_t node_id;
  DataSetItem *item;
  char *buf;
  const HashMeta meta;
  offset_t node_off;
};

struct ValidateRead {
  node_id_t node_id;
  DataSetItem *item;
  char *cas_buf;
  char *version_buf;
  bool has_lock_in_validate;
};

struct CommitWrite {
  node_id_t node_id;
  uint64_t lock_off;
};

class DTX {
 public:
  void TxBegin(tx_id_t txid) {
    // context->BeginTask();
    Clean();
    is_ro_tx = true;
    tx_id = txid;
    DrTM_lease = 0;
    start_time = 0;
    last_write_lock_time = 0;
  }

  void AddToReadOnlySet(DataItemPtr item) {
    DataSetItem data_set_item{.item_ptr = std::move(item),
                              .is_fetched = false,
                              .is_logged = false,
                              .read_which_node = -1};
    read_only_set.emplace_back(data_set_item);
  }

  void AddToReadWriteSet(DataItemPtr item) {
    DataSetItem data_set_item{.item_ptr = std::move(item),
                              .is_fetched = false,
                              .is_logged = false,
                              .read_which_node = -1};
    read_write_set.emplace_back(data_set_item);
  }

  bool TxExe() {
    if (read_write_set.empty() && read_only_set.empty()) {
      return true;
    }
    if (txn_sys == DTX_SYS::OOCC || txn_sys == DTX_SYS::OCC) {
      if (read_write_set.empty()) {
        if (ExeRO()) {
          return true;
        } else {
          goto ABORT;
        }
      } else {
        if (ExeRW()) {
          return true;
        } else {
          goto ABORT;
        }
      }
    } else if (txn_sys == DTX_SYS::DrTMH) {
    } else {
    }

    return true;
  ABORT:
    Abort();
    return false;
  }

  bool TxCommit() {
    if (txn_sys == DTX_SYS::OOCC) {
      auto end_time = get_clock_sys_time_us();
      // SDS_INFO("commit time = %ld, lease = %ld", end_time - start_time,
      // lease);

      // sleep(1);
      // if ((end_time - start_time) > lease) {
      //   if (!Validate()) {
      //     goto ABORT;
      //   }
      // }
      if (read_write_set.size() != 0) {
        if (CoalescentCommit()) {
          // context->EndTask();
          return true;
        } else {
          goto ABORT;
        }
      }
    } else if (txn_sys == DTX_SYS::OCC) {
      if (!Validate()) {
        goto ABORT;
      }
      if (read_write_set.size() != 0) {
        if (CoalescentCommit()) {
          // context->EndTask();
          return true;
        } else {
          goto ABORT;
        }
      }
    }

    // context->EndTask();
    return true;
  ABORT:
    Abort();
    return false;
  }

 public:
  void TxAbortReadOnly() {
    assert(read_write_set.empty());
    read_only_set.clear();
    // context->RetryTask();
    // context->EndTask();
  }

  void TxAbortReadWrite() { Abort(); }

  void RemoveLastROItem() { read_only_set.pop_back(); }

 public:
  DTX(DTXContext *context, int _txn_sys, int _lease);

  ~DTX() { Clean(); }

 private:
  bool ExeRO();

  bool ExeRW();

  bool Validate();

  bool CoalescentCommit();

  void Abort();

  void ParallelUndoLog();

  void Clean() {
    read_only_set.clear();
    read_write_set.clear();
    not_eager_locked_rw_set.clear();
    locked_rw_set.clear();
    old_version_for_insert.clear();
    inserted_pos.clear();
  }

 private:
  bool IssueReadOnly(std::vector<DirectRead> &pending_direct_ro,
                     std::vector<HashRead> &pending_hash_ro);

  bool IssueReadLock(std::vector<CasRead> &pending_cas_rw,
                     std::vector<HashRead> &pending_hash_rw,
                     std::vector<InsertOffRead> &pending_insert_off_rw);

  bool IssueValidate(std::vector<ValidateRead> &pending_validate);

  bool IssueCommitAllSelectFlush(std::vector<CommitWrite> &pending_commit_write,
                                 char *cas_buf);

 private:
  bool CheckDirectRO(std::vector<DirectRead> &pending_direct_ro,
                     std::list<HashRead> &pending_next_hash_ro);

  bool CheckHashRO(std::vector<HashRead> &pending_hash_ro,
                   std::list<HashRead> &pending_next_hash_ro);

  bool CheckNextHashRO(std::list<HashRead> &pending_next_hash_ro);

  bool CheckCasRW(std::vector<CasRead> &pending_cas_rw,
                  std::list<HashRead> &pending_next_hash_rw,
                  std::list<InsertOffRead> &pending_next_off_rw);

  int FindMatchSlot(HashRead &res);

  bool CheckHashRW(std::vector<HashRead> &pending_hash_rw,
                   std::list<HashRead> &pending_next_hash_rw);

  bool CheckNextHashRW(std::list<HashRead> &pending_next_hash_rw);

  int FindInsertOff(InsertOffRead &res);

  bool CheckInsertOffRW(std::vector<InsertOffRead> &pending_insert_off_rw,
                        std::list<InsertOffRead> &pending_next_off_rw);

  bool CheckNextOffRW(std::list<InsertOffRead> &pending_next_off_rw);

 private:
  char *AllocLocalBuffer(size_t size) { return context->Alloc(size); }

  node_id_t GetPrimaryNodeID(table_id_t table_id) {
    return context->GetPrimaryNodeID(table_id);
  }

  std::vector<node_id_t> *GetBackupNodeID(table_id_t table_id) {
    return context->GetBackupNodeID(table_id);
  }

  HashMeta &GetPrimaryHashMetaWithTableID(table_id_t table_id) {
    return context->GetPrimaryHashMetaWithTableID(table_id);
  }

  std::vector<HashMeta> *GetBackupHashMetasWithTableID(table_id_t table_id) {
    return context->GetBackupHashMetasWithTableID(table_id);
  }

  offset_t GetNextLogOffset(node_id_t node_id, size_t log_size) {
    return context->GetNextLogOffset(node_id, log_size);
  }

 public:
  tx_id_t tx_id;

  AddrCache *addr_cache;

 private:
  t_id_t t_id;
  int txn_sys;
  int lease;
  long long start_time;
  long long last_write_lock_time;
  uint64_t DrTM_lease;

  DTXContext *context;

  bool is_ro_tx;
  std::vector<DataSetItem> read_only_set;
  std::vector<DataSetItem> read_write_set;
  std::vector<size_t> not_eager_locked_rw_set;
  std::vector<size_t> locked_rw_set;
  std::vector<OldVersionForInsert> old_version_for_insert;

  struct pair_hash {
    inline std::size_t operator()(
        const std::pair<node_id_t, offset_t> &v) const {
      return v.first * 31 + v.second;
    }
  };

  std::unordered_set<std::pair<node_id_t, offset_t>, pair_hash> inserted_pos;
};
