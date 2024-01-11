

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>

#include "../dtx.h"
#include "tao.h"

using namespace std::placeholders;

#define RetryUntilSuccess 0

size_t kMaxTransactions = 10000;
pthread_barrier_t barrier;
uint64_t threads;
uint64_t coroutines;

int data_item_size;
int write_ratio;
bool is_skewed;
bool delayed;
int lease;
int txn_sys;
thread_local uint64_t tx_id_local = 3;
std::atomic<uint64_t> attempts(0);
std::atomic<uint64_t> commits(0);
std::atomic<uint64_t> attempts_read_only(0);
std::atomic<uint64_t> commits_read_only(0);
double *timer;
std::atomic<uint64_t> tx_id_generator(0);

thread_local size_t ATTEMPTED_NUM;
thread_local uint64_t seed;
TAO *tao_client;
thread_local bool *workgen_arr;

thread_local uint64_t rdma_cnt;
std::atomic<uint64_t> rdma_cnt_sum(0);

bool TxTAO(tx_id_t tx_id, DTX *dtx, uint64_t *att_read_only) {
  dtx->TxBegin(tx_id);
  // random a key
  uint64_t index = FastRand(&seed) % 100000;

  vector<tao_key_t> keys = tao_client->query[index];
  bool read_only = keys[0].read_only;
  // bool read_only = true;
  // cout << "transaction size = " << keys.size() << endl;
  for (int i = 0; i < keys.size(); i++) {
    // for (int i = 0; i < 1; i++) {
    DataItemPtr micro_obj =
        std::make_shared<DataItem>(keys[i].table_id, keys[i].key);
    if (read_only) {
      dtx->AddToReadOnlySet(micro_obj);
    } else {
      dtx->AddToReadWriteSet(micro_obj);
    }
  }
  bool commit_status = true;

  if (!dtx->TxExe()) return false;
  // Commit transaction
  commit_status = dtx->TxCommit();

  return commit_status;
}

thread_local int running_tasks;

void WarmUp(DTXContext *context) {
  DTX *dtx = new DTX(context, txn_sys, lease, delayed);
  bool tx_committed = false;
  uint64_t x = 0;
  for (int i = 0; i < 50000; ++i) {
    uint64_t iter = ++tx_id_local;
    TxTAO(iter, dtx, &x);
  }
  delete dtx;
}

const static uint64_t kCpuFrequency = 2400;
uint64_t g_idle_cycles = 0;

// static void IdleExecution() {
//   if (g_idle_cycles) {
//     uint64_t start_tsc = rdtsc();
//     while (rdtsc() - start_tsc < g_idle_cycles) {
//       YieldTask();
//     }
//   }
// }

void RunTx(DTXContext *context) {
  DTX *dtx = new DTX(context, txn_sys, lease, delayed);
  // struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;
  uint64_t attempt_tx = 0;
  uint64_t commit_tx = 0;
  uint64_t commit_read_only = 0;
  uint64_t attempt_read_only = 0;
  tx_id_local = (uint64_t)GetThreadID() << 45;
  int timer_idx = GetThreadID() * coroutines + GetTaskID();
  struct timespec tx_start_time;
  struct timespec tx_end_time;
  // Running transactions
  while (true) {
    tx_id_local += 1;
    uint64_t iter = tx_id_local;
    attempt_tx++;
    // SDS_INFO("attempt = %ld, %ld", attempt_tx, ATTEMPTED_NUM);

    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    tx_committed = TxTAO(iter, dtx, &attempt_read_only);
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec =
          (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 +
          (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      // SDS_INFO("tx_usec=%lf", tx_usec);
      timer[timer_idx] = tx_usec;
      timer_idx += threads * coroutines;
      commit_tx++;

      // IdleExecution();
    }
    //  else {
    //   SDS_INFO("not found?");
    // }
    // Stat after a million of transactions finish
    if (attempt_tx == ATTEMPTED_NUM) {
      attempts.fetch_add(attempt_tx);
      commits.fetch_add(commit_tx);
      attempts_read_only.fetch_add(attempt_read_only);
      commits_read_only.fetch_add(commit_read_only);
      break;
    }
  }
  running_tasks--;
  delete dtx;
}

void execute_thread(int id, DTXContext *context) {
  BindCore(id);

  // tao_client = new TAO();

  ATTEMPTED_NUM = kMaxTransactions / threads / coroutines;
  auto hostname = GetHostName();
  seed = MurmurHash3_x86_32(hostname.c_str(), hostname.length(), 0xcc9e2d51) *
             kMaxThreads +
         id;
  WarmUp(context);
  // SDS_INFO("warm done");
  TaskPool::Enable();
  auto &task_pool = TaskPool::Get();
  running_tasks = coroutines;
  pthread_barrier_wait(&barrier);
  task_pool.spawn(context->GetPollTask(running_tasks));
  for (int i = 0; i < coroutines; ++i) {
    task_pool.spawn(std::bind(&RunTx, context), 128 * 1024);
  }
  while (!task_pool.empty()) {
    YieldTask();
  }
  pthread_barrier_wait(&barrier);
  rdma_cnt_sum += rdma_cnt;
}

void synchronize_begin(DTXContext *ctx) {
  if (getenv("COMPUTE_NODES")) {
    int nr_compute_nodes = (int)atoi(getenv("COMPUTE_NODES"));
    if (nr_compute_nodes <= 1) return;
    GlobalAddress addr(0, sizeof(SuperChunk));
    uint64_t *buf = (uint64_t *)ctx->Alloc(8);
    ctx->FetchAndAdd(buf, addr, 1);
    ctx->PostRequest();
    ctx->Sync();
    int retry = 0;
    while (true) {
      ctx->read(buf, addr, sizeof(uint64_t));
      ctx->PostRequest();
      ctx->Sync();
      if (*buf == nr_compute_nodes) {
        SDS_INFO("ticket = %ld", *buf);
        break;
      }
      sleep(1);
      retry++;
      SDS_INFO("FAILED ticket = %ld", *buf);
      assert(retry < 60);
    }
  }
}

void synchronize_end(DTXContext *ctx) {
  if (getenv("COMPUTE_NODES")) {
    uint64_t *buf = (uint64_t *)ctx->Alloc(8);
    int nr_compute_nodes = (int)atoi(getenv("COMPUTE_NODES"));
    if (nr_compute_nodes <= 1) return;
    GlobalAddress addr(0, sizeof(SuperChunk));
    ctx->CompareAndSwap(buf, addr, nr_compute_nodes, 0);
    ctx->PostRequest();
    ctx->Sync();
    if (*buf == nr_compute_nodes) {
      SDS_INFO("RESET ticket = 0");
    }
  }
}

// void report_per_second() {
//   uint64_t last_committed = 0;
//   while (true) {
//     sleep(1);
//     uint64_t now = commits.load();
//     SDS_INFO("%.3lf", (now - last_committed) / 1000000.0);
//     last_committed = now;
//   }
// }

void report(double elapsed_time, JsonConfig &config) {
  assert(commits.load() <= kMaxTransactions);
  std::sort(timer, timer + commits.load());
  std::string dump_prefix;
  // if (getenv("DUMP_PREFIX")) {
  //   dump_prefix = std::string(getenv("DUMP_PREFIX"));
  // } else {
  //   dump_prefix = "dtx-ycsb";
  // }
  SDS_INFO(
      "%s: #thread = %ld, #coro_per_thread = %ld, "
      "attempt txn = %.3lf M/s, committed txn = %.3lf M/s, "
      "P50 latency = %.3lf us, P99 latency = %.3lf us, abort rate = %.3lf, "
      "RDMA ops per txn = %.3lf M, RDMA ops per second = %.3lf M",
      dump_prefix.c_str(), threads, coroutines, attempts.load() / elapsed_time,
      commits.load() / elapsed_time, timer[(int)(0.5 * commits.load())],
      timer[(int)(0.99 * commits.load())],
      1.0 - (commits.load() * 1.0 / attempts.load()),
      1.0 * rdma_cnt_sum.load() / attempts.load(),
      rdma_cnt_sum.load() / elapsed_time);
  // std::string dump_file_path = config.get("dumps_file_path").get_str();
  // if (getenv("DUMP_FILE_PATH")) {
  //   dump_file_path = getenv("DUMP_FILE_PATH");
  // }
  // if (dump_file_path.empty()) {
  //   return;
  // }
  // FILE *fout = fopen(dump_file_path.c_str(), "a+");
  // if (!fout) {
  //   SDS_PERROR("fopen");
  //   return;
  // }
  // fprintf(fout,
  //         "%s, %ld, %ld, %.3lf, %.3lf, %.3lf, %.3lf, %.3lf, %.3lf,
  //             % .3lf\n
  //             ", dump_prefix.c_str(), threads, coroutines, attempts.load() /
  //             elapsed_time,
  //         commits.load() / elapsed_time, timer[(int)(0.5 * commits.load())],
  //         timer[(int)(0.99 * commits.load())],
  //         1.0 - (commits.load() * 1.0 / attempts.load()),
  //         1.0 * rdma_cnt_sum.load() / attempts.load(),
  //         rdma_cnt_sum.load() / elapsed_time);
  // fclose(fout);
}

int main(int argc, char **argv) {
  BindCore(1);
  const char *path = ROOT_DIR "/config/transaction.json";
  if (getenv("APP_CONFIG_PATH")) {
    path = getenv("APP_CONFIG_PATH");
  }
  if (getenv("IDLE_USEC")) {
    g_idle_cycles = kCpuFrequency * atoi(getenv("IDLE_USEC"));
  }

  JsonConfig config = JsonConfig::load_file(path);
  kMaxTransactions = config.get("nr_transactions").get_uint64();
  lease = config.get("lease").get_uint64();
  txn_sys = config.get("txn_sys").get_uint64();
  if (txn_sys == DTX_SYS::OOCC) {
    SDS_INFO("running OOCC");
  } else if (txn_sys == DTX_SYS::OCC) {
    SDS_INFO("running OCC");
  } else if (txn_sys == DTX_SYS::DrTMH) {
    SDS_INFO("running DrTM");
  } else {
    SDS_INFO("running DSLR");
  }
  tao_client = new TAO();
  tao_client->GenerateQuery();
  delayed = config.get("delayed").get_bool();
  srand48(time(nullptr));
  threads = argc < 2 ? 1 : atoi(argv[1]);
  coroutines = argc < 3 ? 1 : atoi(argv[2]);
  timer = new double[kMaxTransactions];
  DTXContext *context = new DTXContext(config, threads);
  SDS_INFO("context init done");
  context->addr_cache.resize();
  timespec ts_begin, ts_end;
  pthread_barrier_init(&barrier, nullptr, threads + 1);
  std::vector<std::thread> workers;
  workers.resize(threads);
  synchronize_begin(context);
  for (int i = 0; i < threads; ++i) {
    workers[i] = std::thread(execute_thread, i, context);
  }

  // std::thread(report_per_second);
  pthread_barrier_wait(&barrier);
  clock_gettime(CLOCK_MONOTONIC, &ts_begin);
  pthread_barrier_wait(&barrier);
  clock_gettime(CLOCK_MONOTONIC, &ts_end);
  for (int i = 0; i < threads; ++i) {
    workers[i].join();
  }
  double elapsed_time = (ts_end.tv_sec - ts_begin.tv_sec) * 1000000.0 +
                        (ts_end.tv_nsec - ts_begin.tv_nsec) / 1000.0;
  report(elapsed_time, config);
  synchronize_end(context);
  delete context;
  return 0;
}