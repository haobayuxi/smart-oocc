// Some contents of this file are derived from FORD
// https://github.com/minghust/FORD

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>

#include "../dtx.h"
#include "tpcc.h"

using namespace std::placeholders;

size_t kMaxTransactions = 1000000;
pthread_barrier_t barrier;
uint64_t threads;
uint64_t coroutines;

std::atomic<uint64_t> attempts(0);
std::atomic<uint64_t> commits(0);
double *timer;
std::atomic<uint64_t> tx_id_generator(0);

int lease;
int txn_sys;

thread_local size_t ATTEMPTED_NUM;
thread_local uint64_t seed;
thread_local TPCC *tatp_client;
thread_local uint64_t tx_id_local;
thread_local TPCCTxType *workgen_arr;

thread_local uint64_t rdma_cnt;
std::atomic<uint64_t> rdma_cnt_sum(0);

thread_local int running_tasks;

void WarmUp(DTXContext *context) {
  DTX *dtx = new DTX(context, txn_sys, lease);
  bool tx_committed = false;
  for (int i = 0; i < 50000; ++i) {
    TATPTxType tx_type = workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_local;
    // Global atomic transaction id
    switch (tx_type) {
      case TATPTxType::kGetSubsciberData:
        tx_committed = TxGetSubsciberData(iter, dtx);
        break;
      case TATPTxType::kGetNewDestination:
        tx_committed = TxGetNewDestination(iter, dtx);
        break;
      case TATPTxType::kGetAccessData:
        tx_committed = TxGetAccessData(iter, dtx);
        break;
      case TATPTxType::kUpdateSubscriberData:
        tx_committed = TxUpdateSubscriberData(iter, dtx);
        break;
      case TATPTxType::kUpdateLocation:
        tx_committed = TxUpdateLocation(iter, dtx);
        break;
      case TATPTxType::kInsertCallForwarding:
        tx_committed = TxInsertCallForwarding(iter, dtx);
        break;
      case TATPTxType::kDeleteCallForwarding:
        tx_committed = TxDeleteCallForwarding(iter, dtx);
        break;
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    // SDS_INFO("warm up id=%ld, committed=%d", iter, tx_committed);
  }
  delete dtx;
}

const static uint64_t kCpuFrequency = 2400;
uint64_t g_idle_cycles = 0;

static void IdleExecution() {
  if (g_idle_cycles) {
    uint64_t start_tsc = rdtsc();
    while (rdtsc() - start_tsc < g_idle_cycles) {
      YieldTask();
    }
  }
}

void RunTx(DTXContext *context) {
  DTX *dtx = new DTX(context, txn_sys, lease);
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;
  uint64_t attempt_tx = 0;
  uint64_t commit_tx = 0;
  tx_id_local = (uint64_t)GetThreadID() << 50;

  int timer_idx = GetThreadID() * coroutines + GetTaskID();
  // Running transactions
  while (true) {
    TATPTxType tx_type = workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_local;  // Global atomic transaction id
    attempt_tx++;
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
#ifdef ABORT_DISCARD
    switch (tx_type) {
      case TATPTxType::kGetSubsciberData:
        do {
          tx_committed = TxGetSubsciberData(iter, dtx);
        } while (!tx_committed);
        break;
      case TATPTxType::kGetNewDestination:
        do {
          tx_committed = TxGetNewDestination(iter, dtx);
        } while (!tx_committed);
        break;
      case TATPTxType::kGetAccessData:
        do {
          tx_committed = TxGetAccessData(iter, dtx);
        } while (!tx_committed);
        break;
      case TATPTxType::kUpdateSubscriberData:
        do {
          tx_committed = TxUpdateSubscriberData(iter, dtx);
        } while (!tx_committed);
        break;
      case TATPTxType::kUpdateLocation:
        do {
          tx_committed = TxUpdateLocation(iter, dtx);
        } while (!tx_committed);
        break;
      case TATPTxType::kInsertCallForwarding:
        do {
          tx_committed = TxInsertCallForwarding(iter, dtx);
        } while (!tx_committed);
        break;
      case TATPTxType::kDeleteCallForwarding:
        do {
          tx_committed = TxDeleteCallForwarding(iter, dtx);
        } while (!tx_committed);
        break;
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
#else
    switch (tx_type) {
      case TATPTxType::kGetSubsciberData:
        SDS_INFO("get sub");
        tx_committed = TxGetSubsciberData(iter, dtx);
        break;
      case TATPTxType::kGetNewDestination:
        SDS_INFO("get new dest");
        tx_committed = TxGetNewDestination(iter, dtx);
        break;
      case TATPTxType::kGetAccessData:
        SDS_INFO("get get access");
        tx_committed = TxGetAccessData(iter, dtx);
        break;
      case TATPTxType::kUpdateSubscriberData:
        SDS_INFO("update sub");
        tx_committed = TxUpdateSubscriberData(iter, dtx);
        break;
      case TATPTxType::kUpdateLocation:
        SDS_INFO("update location");
        tx_committed = TxUpdateLocation(iter, dtx);
        break;
      case TATPTxType::kInsertCallForwarding:
        SDS_INFO("insert sub");
        tx_committed = TxInsertCallForwarding(iter, dtx);
        break;
      case TATPTxType::kDeleteCallForwarding:
        SDS_INFO("delete sub");
        tx_committed = TxDeleteCallForwarding(iter, dtx);
        break;
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
#endif
    // Stat after one transaction finishes
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec =
          (tx_end_time.tv_sec - tx_start_time.tv_sec) * 1000000 +
          (double)(tx_end_time.tv_nsec - tx_start_time.tv_nsec) / 1000;
      timer[timer_idx] = tx_usec;
      timer_idx += threads * coroutines;
      commit_tx++;
      IdleExecution();
    }
    // Stat after a million of transactions finish
    if (attempt_tx == ATTEMPTED_NUM) {
      attempts.fetch_add(attempt_tx);
      commits.fetch_add(commit_tx);
      break;
    }
  }
  running_tasks--;
  delete dtx;
}

void execute_thread(int id, DTXContext *context) {
  BindCore(id);
#if 0
    assert(coroutines == 1);
    ATTEMPTED_NUM = kMaxTransactions / threads;
    seed = rdtsc() * kMaxThreads + id;
    tatp_client = new TATP();
    workgen_arr = tatp_client->CreateWorkgenArray();
    pthread_barrier_wait(&barrier);
    RunTx(manager);
    pthread_barrier_wait(&barrier);
    rdma_cnt_sum += rdma_cnt;
#endif
  ATTEMPTED_NUM = kMaxTransactions / threads / coroutines;
  auto hostname = GetHostName();
  seed = MurmurHash3_x86_32(hostname.c_str(), hostname.length(), 0xcc9e2d51) *
             kMaxThreads +
         id;
  tatp_client = new TATP();
  workgen_arr = tatp_client->CreateWorkgenArray();
  WarmUp(context);
  //   SDS_INFO("warm up done");
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

void report(double elapsed_time, JsonConfig &config) {
  assert(commits.load() <= kMaxTransactions);
  std::sort(timer, timer + commits.load());
  std::string dump_prefix;
  if (getenv("DUMP_PREFIX")) {
    dump_prefix = std::string(getenv("DUMP_PREFIX"));
  } else {
    dump_prefix = "dtx-tatp";
  }
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
  std::string dump_file_path = config.get("dump_file_path").get_str();
  if (getenv("DUMP_FILE_PATH")) {
    dump_file_path = getenv("DUMP_FILE_PATH");
  }
  if (dump_file_path.empty()) {
    return;
  }
  FILE *fout = fopen(dump_file_path.c_str(), "a+");
  if (!fout) {
    SDS_PERROR("fopen");
    return;
  }
  fprintf(
      fout, "%s, %ld, %ld, %.3lf, %.3lf, %.3lf, %.3lf, %.3lf, %.3lf, %.3lf\n",
      dump_prefix.c_str(), threads, coroutines, attempts.load() / elapsed_time,
      commits.load() / elapsed_time, timer[(int)(0.5 * commits.load())],
      timer[(int)(0.99 * commits.load())],
      1.0 - (commits.load() * 1.0 / attempts.load()),
      1.0 * rdma_cnt_sum.load() / attempts.load(),
      rdma_cnt_sum.load() / elapsed_time);
  fclose(fout);
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
  }
  srand48(time(nullptr));
  threads = argc < 2 ? 1 : atoi(argv[1]);
  coroutines = argc < 3 ? 1 : atoi(argv[2]);
  timer = new double[kMaxTransactions];
  DTXContext *context = new DTXContext(config, threads);
  tatp_client = new TATP();
  timespec ts_begin, ts_end;
  pthread_barrier_init(&barrier, nullptr, threads + 1);
  std::vector<std::thread> workers;
  workers.resize(threads);
  synchronize_begin(context);
  for (int i = 0; i < threads; ++i) {
    workers[i] = std::thread(execute_thread, i, context);
  }
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