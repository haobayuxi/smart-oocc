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
  uint64_t prev_maxx;
  uint64_t prev_maxs;
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
  uint64_t prev_maxs;
  uint64_t prev_maxx;
};

struct HashRead {
  node_id_t node_id;
  DataSetItem *item;
  char *buf;
  const HashMeta meta;
};

struct InvisibleRead {
  node_id_t node_id;
  char *buf;
  uint64_t off;
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

struct ResetLock {
  uint64_t offset;
  uint64_t lock;
  char *cas_buf;
};

class DTX {
 public:
  void TxBegin(tx_id_t txid) {
    context->BeginTask();
    context->Sync();
    read_only_set.clear();
    read_write_set.clear();
    Clean();
    reset.clear();
    is_ro_tx = true;
    tx_id = txid;
    re_validate = false;
    start_time = get_clock_sys_time_us();
  }
  ALWAYS_INLINE
  void AddToReadOnlySet(DataItemPtr item) {
    DataSetItem data_set_item{.item_ptr = std::move(item),
                              .is_fetched = false,
                              .is_logged = false,
                              .read_which_node = -1};
    read_only_set.emplace_back(data_set_item);
  }
  ALWAYS_INLINE
  void AddToReadWriteSet(DataItemPtr item) {
    DataSetItem data_set_item{.item_ptr = std::move(item),
                              .is_fetched = false,
                              .is_logged = false,
                              .read_which_node = -1};
    read_write_set.emplace_back(data_set_item);
  }

  bool TxExe(bool fail_abort = true) {
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
      if (read_write_set.empty()) {
        if (DrTMExeRO()) {
          return true;
        } else {
          goto ABORT;
        }
      } else {
        if (DrTMExeRW()) {
          return true;
        } else {
          goto ABORT;
        }
      }
    } else {
      if (read_write_set.empty()) {
        if (DSLRExeRO()) {
          return true;
        } else {
          DSLRAbort();
          return false;
        }
      } else {
        if (DSLRExeRW()) {
          return true;
        } else {
          DSLRAbort();
          context->RetryTask();
          context->EndTask();
          return false;
        }
      }
    }

    return true;
  ABORT:
    if (fail_abort) Abort();
    return false;
  }

  bool TxCommit() {
    auto end_time = get_clock_sys_time_us();
    if (txn_sys == DTX_SYS::DrTMH) {
      end_time = end_time << 1;
      for (auto &item : read_only_set) {
        uint64_t read_lease = item.item_ptr.get()->lock;
        if (read_lease < end_time) {
          // SDS_INFO("commit lease expired, %ld %ld, key%ld", read_lease,
          //          end_time, item.item_ptr.get()->key);
          goto ABORT;
        }
      }
      if (!is_ro_tx) {
        if (CoalescentCommit()) {
          context->EndTask();
          return true;
        } else {
          goto ABORT;
        }
      }

    } else if (txn_sys == DTX_SYS::OOCC) {
      // check lease

      if ((end_time - start_time) > (lease - offset) || re_validate) {
        if (!Validate()) {
          goto ABORT;
        }
      }

      if (!is_ro_tx) {
        if (OOCCCommit()) {
          context->EndTask();
          return true;
        } else {
          goto ABORT;
        }
      }
    } else if (txn_sys == DTX_SYS::OCC) {
      if (is_ro_tx && read_only_set.size() == 1) {
        context->EndTask();
        return true;
      }
      if (!Validate()) {
        goto ABORT;
      }
      if (!is_ro_tx) {
        if (CoalescentCommit()) {
          context->EndTask();
          return true;
        } else {
          goto ABORT;
        }
      }
    } else {
      DSLRCommit();
    }

    context->EndTask();
    return true;
  ABORT:
    Abort();
    return false;
  }

 public:
  void TxAbortReadOnly() {
    assert(read_write_set.empty());
    read_only_set.clear();
    context->RetryTask();
    context->EndTask();
  }

  void TxAbortReadWrite() { Abort(); }

  void RemoveLastROItem() { read_only_set.pop_back(); }

 public:
  DTX(DTXContext *context, int _txn_sys, int _lease, bool _delayed,
      double _offset);

  ~DTX() { Clean(); }

  void Clean() {
    not_eager_locked_rw_set.clear();
    locked_rw_set.clear();
    old_version_for_insert.clear();
    inserted_pos.clear();
  }

 private:
  bool ExeRO();

  bool ExeRW();

  bool Validate();

  bool CoalescentCommit();

  bool OOCCCommit();

  void Abort();

  void ParallelUndoLog();

 private:
  bool IssueReadOnly(std::vector<DirectRead> &pending_direct_ro,
                     std::vector<HashRead> &pending_hash_ro);

  bool IssueReadWrite(std::vector<CasRead> &pending_cas_rw,
                      std::vector<HashRead> &pending_hash_rw,
                      std::vector<InsertOffRead> &pending_insert_off_rw);

  bool IssueValidate(std::vector<ValidateRead> &pending_validate);

  bool IssueCommitAllSelectFlush(std::vector<CommitWrite> &pending_commit_write,
                                 char *cas_buf);

 private:
  bool CheckDirectRO(std::vector<DirectRead> &pending_direct_ro,
                     std::list<DirectRead> pending_next_direct_ro,
                     std::list<HashRead> &pending_next_hash_ro);
  bool CheckNextDirectRO(std::list<DirectRead> &pending_next_direct_ro);
  bool CheckInvisibleRO(std::list<InvisibleRead> &pending_invisible_ro);

  bool CheckHashRO(std::vector<HashRead> &pending_hash_ro,
                   std::list<InvisibleRead> &pending_invisible_ro,
                   std::list<HashRead> &pending_next_hash_ro);

  bool CheckNextHashRO(std::list<HashRead> &pending_next_hash_ro);
  bool CheckNextCasRW(std::list<CasRead> &pending_next_cas_rw);

  bool CheckCasRW(std::vector<CasRead> &pending_cas_rw,
                  std::list<HashRead> &pending_next_hash_rw,
                  std::list<InsertOffRead> &pending_next_off_rw);

  int FindMatchSlot(HashRead &res, std::list<CasRead> &pending_next_hash_rw);

  bool CheckHashRW(std::vector<HashRead> &pending_hash_rw,
                   std::list<CasRead> &pending_next_cas_rw,
                   std::list<HashRead> &pending_next_hash_rw);

  bool CheckNextHashRW(std::list<CasRead> &pending_next_cas_rw,
                       std::list<HashRead> &pending_next_hash_rw);

  int FindInsertOff(InsertOffRead &res,
                    std::list<InvisibleRead> &pending_invisible_ro);

  bool CheckInsertOffRW(std::vector<InsertOffRead> &pending_insert_off_rw,
                        std::list<InvisibleRead> &pending_invisible_ro,
                        std::list<InsertOffRead> &pending_next_off_rw);

  bool CheckNextOffRW(std::list<InvisibleRead> &pending_invisible_ro,
                      std::list<InsertOffRead> &pending_next_off_rw);

  // DrTM
  bool lease_expired(uint64_t lease);
  bool DrTMExeRO();
  bool DrTMExeRW();
  bool DrTMCheckDirectRO(std::vector<CasRead> &pending_cas_ro,
                         std::list<CasRead> &pending_next_cas_ro,
                         std::list<HashRead> &pending_next_hash_ro);
  bool DrTMCheckNextCasRO(std::list<CasRead> &pending_next_cas_ro);
  bool DrTMCheckHashRO(std::vector<HashRead> &pending_hash_ro,
                       std::list<CasRead> &pending_next_cas_ro,
                       std::list<HashRead> &pending_next_hash_ro);
  bool DrTMIssueReadOnly(std::vector<CasRead> &pending_cas_ro,
                         std::vector<HashRead> &pending_hash_ro);
  bool DrTMIssueReadWrite(std::vector<CasRead> &pending_cas_rw,
                          std::vector<HashRead> &pending_hash_rw,
                          std::vector<InsertOffRead> &pending_insert_off_rw);
  bool DrTMCheckHashRW(std::vector<HashRead> &pending_hash_rw,
                       std::list<CasRead> &pending_next_cas_rw,
                       std::list<HashRead> &pending_next_hash_rw);
  bool DrTMCheckNextHashRW(std::list<CasRead> &pending_next_cas_rw,
                           std::list<HashRead> &pending_next_hash_rw);
  bool DrTMCheckCasRW(std::vector<CasRead> &pending_cas_rw,
                      std::list<CasRead> &pending_next_cas_rw,
                      std::list<HashRead> &pending_next_hash_rw,
                      std::list<InsertOffRead> &pending_next_off_rw);
  bool DrTMCheckNextCasRW(std::list<CasRead> &pending_next_cas_rw);
  bool DrTMCheckNextHashRO(std::list<HashRead> &pending_next_hash_ro,
                           std::list<CasRead> &pending_next_cas_ro);

  //   DSLR
  bool DSLRExeRO();
  bool DSLRExeRW();
  bool DSLRCommit();
  bool DSLRAbort();
  bool CheckReset();
  bool DSLRCheckDirectRO(std::list<DirectRead> &pending_next_direct_ro);
  bool DSLRCheckDirectRW(std::list<DirectRead> &pending_next_direct_rw);
  bool DSLRCheckCasRO(std::vector<CasRead> &pending_cas_ro,
                      std::list<DirectRead> &pending_next_direct_ro,
                      std::list<HashRead> &pending_next_hash_ro);
  bool DSLRCheckNextCasRO(std::list<CasRead> &pending_next_cas_ro,
                          std::list<DirectRead> pending_next_direct_ro);
  bool DSLRCheckHashRO(std::vector<HashRead> &pending_hash_ro,
                       std::list<CasRead> &pending_next_cas_ro,
                       std::list<HashRead> &pending_next_hash_ro);
  bool DSLRCheckNextHashRO(std::list<CasRead> &pending_next_cas_ro,
                           std::list<HashRead> &pending_next_hash_ro);
  bool DSLRIssueReadOnly(std::vector<CasRead> &pending_cas_ro,
                         std::vector<HashRead> &pending_hash_ro);
  bool DSLRIssueReadWrite(std::vector<CasRead> &pending_cas_rw,
                          std::vector<HashRead> &pending_hash_rw,
                          std::vector<InsertOffRead> &pending_insert_off_rw);
  bool DSLRCheckHashRW(std::vector<HashRead> &pending_hash_rw,
                       std::list<CasRead> &pending_next_cas_rw,
                       std::list<HashRead> &pending_next_hash_rw);
  bool DSLRCheckNextHashRW(std::list<CasRead> &pending_next_cas_rw,
                           std::list<HashRead> &pending_next_hash_rw);
  bool DSLRCheckCasRW(std::vector<CasRead> &pending_cas_rw,
                      std::list<HashRead> &pending_next_hash_rw,
                      std::list<DirectRead> &pending_next_direct_rw,
                      std::list<InsertOffRead> &pending_next_off_rw);
  bool DSLRCheckNextCasRW(std::list<CasRead> &pending_next_cas_rw,
                          std::list<DirectRead> pending_next_direct_rw);
  // int check_read_lock(uint64_t lock, uint64_t offset);
  // int check_write_lock1(uint64_t lock, uint64_t offset);

 private:
  char *AllocLocalBuffer(size_t size) { return context->Alloc(size); }

  ALWAYS_INLINE
  node_id_t GetPrimaryNodeID(uint64_t key) {
    return context->GetPrimaryNodeID(key);
    // get by
  }

  std::vector<node_id_t> *GetBackupNodeID(table_id_t table_id) {
    return context->GetBackupNodeID(table_id);
  }
  ALWAYS_INLINE
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
  int lease;
  int txn_sys;
  double offset;
  bool delay_lock;
  // bool delay_unlock
  uint64_t last_write_lock_time;
  bool re_validate;

  t_id_t t_id;
  std::vector<DataSetItem> read_only_set;
  std::vector<DataSetItem> read_write_set;

  AddrCache *addr_cache;

 private:
  tx_id_t tx_id;
  uint64_t start_time;

  DTXContext *context;

  bool is_ro_tx;
  std::vector<size_t> not_eager_locked_rw_set;
  std::vector<size_t> locked_rw_set;
  std::list<ResetLock> reset;
  std::vector<OldVersionForInsert> old_version_for_insert;
  struct pair_hash {
    inline std::size_t operator()(
        const std::pair<node_id_t, offset_t> &v) const {
      return v.first * 31 + v.second;
    }
  };

  std::unordered_set<std::pair<node_id_t, offset_t>, pair_hash> inserted_pos;
};