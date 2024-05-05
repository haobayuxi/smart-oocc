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
double* timer;
std::atomic<uint64_t> tx_id_generator(0);

int lease;
int txn_sys;
bool delayed;
double offset;

thread_local size_t ATTEMPTED_NUM;
thread_local uint64_t seed;
thread_local TPCC* tpcc_client;
thread_local uint64_t tx_id_local;
thread_local TPCCTxType* workgen_arr;

thread_local uint64_t rdma_cnt;
std::atomic<uint64_t> rdma_cnt_sum(0);

thread_local int running_tasks;

bool TxNewOrder(tx_id_t tx_id, DTX* dtx) {
  /*
  "NEW_ORDER": {
  "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
  "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND
  D_W_ID = ?", # d_id, w_id "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT
  FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id,
  c_id "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID =
  ? AND D_W_ID = ?", # d_next_o_id, d_id, w_id "createOrder": "INSERT INTO
  ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT,
  O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", # d_next_o_id, d_id, w_id,
  c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local "createNewOrder": "INSERT
  INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", # o_id, d_id,
  w_id "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?",
  # ol_i_id "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT,
  S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id,
  ol_i_id, ol_supply_w_id "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD
  = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", #
  s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
  "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID,
  OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT,
  OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id,
  ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
  },
  */

  dtx->TxBegin(tx_id);

  // Generate parameters

  int warehouse_id_start_ = 1;
  int warehouse_id_end_ = tpcc_client->num_warehouse;

  int district_id_start = 1;
  int district_id_end_ = tpcc_client->num_district_per_warehouse;

  const uint32_t warehouse_id = tpcc_client->PickWarehouseId(
      tpcc_client->r, warehouse_id_start_, warehouse_id_end_);
  const uint32_t district_id = tpcc_client->RandomNumber(
      tpcc_client->r, district_id_start, district_id_end_);
  const uint32_t customer_id = tpcc_client->GetCustomerId(tpcc_client->r);
  int64_t c_key =
      tpcc_client->MakeCustomerKey(warehouse_id, district_id, customer_id);

  int32_t all_local = 1;
  std::set<uint64_t> stock_set;  // remove identity stock ids;

  // local buffer used store stocks
  int64_t remote_stocks[tpcc_order_line_val_t::MAX_OL_CNT],
      local_stocks[tpcc_order_line_val_t::MAX_OL_CNT];
  int64_t remote_item_ids[tpcc_order_line_val_t::MAX_OL_CNT],
      local_item_ids[tpcc_order_line_val_t::MAX_OL_CNT];
  uint32_t local_supplies[tpcc_order_line_val_t::MAX_OL_CNT],
      remote_supplies[tpcc_order_line_val_t::MAX_OL_CNT];

  int num_remote_stocks(0), num_local_stocks(0);

  const int num_items = tpcc_client->RandomNumber(
      tpcc_client->r, tpcc_order_line_val_t::MIN_OL_CNT,
      tpcc_order_line_val_t::MAX_OL_CNT);

  for (int i = 0; i < num_items; i++) {
    int64_t item_id = tpcc_client->GetItemId(tpcc_client->r);
    if (tpcc_client->num_warehouse == 1 ||
        tpcc_client->RandomNumber(tpcc_client->r, 1, 100) >
            g_new_order_remote_item_pct) {
      // local stock case
      uint32_t supplier_warehouse_id = warehouse_id;
      int64_t s_key = tpcc_client->MakeStockKey(supplier_warehouse_id, item_id);
      if (stock_set.find(s_key) != stock_set.end()) {
        i--;
        continue;
      } else {
        stock_set.insert(s_key);
      }
      local_supplies[num_local_stocks] = supplier_warehouse_id;
      local_item_ids[num_local_stocks] = item_id;
      local_stocks[num_local_stocks++] = s_key;
    } else {
      // remote stock case
      int64_t s_key;
      uint32_t supplier_warehouse_id;
      do {
        supplier_warehouse_id = tpcc_client->RandomNumber(
            tpcc_client->r, 1, tpcc_client->num_warehouse);
      } while (supplier_warehouse_id == warehouse_id);

      all_local = 0;

      s_key = tpcc_client->MakeStockKey(supplier_warehouse_id, item_id);
      if (stock_set.find(s_key) != stock_set.end()) {
        i--;
        continue;
      } else {
        stock_set.insert(s_key);
      }
      remote_stocks[num_remote_stocks] = s_key;
      remote_supplies[num_remote_stocks] = supplier_warehouse_id;
      remote_item_ids[num_remote_stocks++] = item_id;
    }
  }

  // Run

  tpcc_warehouse_key_t ware_key;
  ware_key.w_id = warehouse_id;
  auto ware_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kWarehouseTable, ware_key.item_key);
  dtx->AddToReadOnlySet(ware_obj);

  tpcc_customer_key_t cust_key;
  cust_key.c_id = c_key;
  auto cust_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kCustomerTable, cust_key.item_key);
  dtx->AddToReadOnlySet(cust_obj);

  // read and update district value
  uint64_t d_key = tpcc_client->MakeDistrictKey(warehouse_id, district_id);
  tpcc_district_key_t dist_key;
  dist_key.d_id = d_key;
  auto dist_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kDistrictTable, dist_key.item_key);
  dtx->AddToReadWriteSet(dist_obj);

  if (!dtx->TxExe()) return false;

  auto* ware_val = (tpcc_warehouse_val_t*)ware_obj->value;
  std::string check(ware_val->w_zip);
  if (check != tpcc_zip_magic) {
    // SDS_PERROR << "[FATAL] Read warehouse unmatch, tid-cid-txid: " <<
    // dtx->t_id
    //            << "-" << dtx->coro_id << "-" << tx_id;
  }

  auto* cust_val = (tpcc_customer_val_t*)cust_obj->value;
  // c_since never be 0
  if (cust_val->c_since == 0) {
    // SDS_PERROR << "[FATAL] Read customer unmatch, tid-cid-txid: " <<
    // dtx->t_id
    //            << "-" << dtx->coro_id << "-" << tx_id;
  }

  tpcc_district_val_t* dist_val = (tpcc_district_val_t*)dist_obj->value;
  check = std::string(dist_val->d_zip);
  if (check != tpcc_zip_magic) {
    // SDS_PERROR << "[FATAL] Read district unmatch, tid-cid-txid: " <<
    // dtx->t_id
    //            << "-" << dtx->coro_id << "-" << tx_id;
  }

  const auto my_next_o_id = dist_val->d_next_o_id;

  dist_val->d_next_o_id++;

  // insert neworder record
  uint64_t no_key =
      tpcc_client->MakeNewOrderKey(warehouse_id, district_id, my_next_o_id);
  tpcc_new_order_key_t norder_key;
  norder_key.no_id = no_key;
  auto norder_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kNewOrderTable, sizeof(tpcc_new_order_val_t),
      norder_key.item_key, tx_id, 1);
  dtx->AddToReadWriteSet(norder_obj);

  // insert order record
  uint64_t o_key =
      tpcc_client->MakeOrderKey(warehouse_id, district_id, my_next_o_id);
  tpcc_order_key_t order_key;
  order_key.o_id = o_key;
  auto order_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kOrderTable, sizeof(tpcc_order_val_t),
      order_key.item_key, tx_id, 1);
  dtx->AddToReadWriteSet(order_obj);

  // insert order index record
  uint64_t o_index_key = tpcc_client->MakeOrderIndexKey(
      warehouse_id, district_id, customer_id, my_next_o_id);
  tpcc_order_index_key_t order_index_key;
  order_index_key.o_index_id = o_index_key;
  auto oidx_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kOrderIndexTable,
      sizeof(tpcc_order_index_val_t), order_index_key.item_key, tx_id, 1);
  dtx->AddToReadWriteSet(oidx_obj);

  if (!dtx->TxExe()) return false;

  // Respectively assign values
  tpcc_new_order_val_t* norder_val = (tpcc_new_order_val_t*)norder_obj->value;
  norder_val->debug_magic = tpcc_add_magic;

  tpcc_order_val_t* order_val = (tpcc_order_val_t*)order_obj->value;
  order_val->o_c_id = int32_t(customer_id);
  order_val->o_carrier_id = 0;
  order_val->o_ol_cnt = num_items;
  order_val->o_all_local = all_local;
  order_val->o_entry_d = tpcc_client->GetCurrentTimeMillis();

  tpcc_order_index_val_t* oidx_val = (tpcc_order_index_val_t*)oidx_obj->value;
  oidx_val->o_id = o_key;
  oidx_val->debug_magic = tpcc_add_magic;

  // -----------------------------------------------------------------------------
  for (int ol_number = 1; ol_number <= num_local_stocks; ol_number++) {
    const int64_t ol_i_id = local_item_ids[ol_number - 1];
    const uint32_t ol_quantity =
        tpcc_client->RandomNumber(tpcc_client->r, 1, 10);
    // read item info
    tpcc_item_key_t tpcc_item_key;
    tpcc_item_key.i_id = ol_i_id;

    auto item_obj = std::make_shared<DataItem>(
        (table_id_t)TPCCTableType::kItemTable, tpcc_item_key.item_key);
    dtx->AddToReadOnlySet(item_obj);

    int64_t s_key = local_stocks[ol_number - 1];
    // read and update stock info
    tpcc_stock_key_t stock_key;
    stock_key.s_id = s_key;

    auto stock_obj = std::make_shared<DataItem>(
        (table_id_t)TPCCTableType::kStockTable, stock_key.item_key);
    dtx->AddToReadWriteSet(stock_obj);

    if (!dtx->TxExe()) return false;

    tpcc_item_val_t* item_val = (tpcc_item_val_t*)item_obj->value;
    tpcc_stock_val_t* stock_val = (tpcc_stock_val_t*)stock_obj->value;

    if (item_val->debug_magic != tpcc_add_magic) {
      // SDS_PERROR << "[FATAL] Read item unmatch, tid-cid-txid: " << dtx->t_id
      //            << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (stock_val->debug_magic != tpcc_add_magic) {
      // SDS_PERROR << "[FATAL] Read stock unmatch, tid-cid-txid: " << dtx->t_id
      //            << "-" << dtx->coro_id << "-" << tx_id;
    }

    if (stock_val->s_quantity - ol_quantity >= 10) {
      stock_val->s_quantity -= ol_quantity;
    } else {
      stock_val->s_quantity += (-int32_t(ol_quantity) + 91);
    }

    stock_val->s_ytd += ol_quantity;
    stock_val->s_remote_cnt +=
        (local_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;

    // insert order line record
    int64_t ol_key = tpcc_client->MakeOrderLineKey(warehouse_id, district_id,
                                                   my_next_o_id, ol_number);
    tpcc_order_line_key_t order_line_key;
    order_line_key.ol_id = ol_key;
    auto ol_obj = std::make_shared<DataItem>(
        (table_id_t)TPCCTableType::kOrderLineTable,
        sizeof(tpcc_order_line_val_t), order_line_key.item_key, tx_id, 1);
    dtx->AddToReadWriteSet(ol_obj);

    if (!dtx->TxExe()) return false;

    tpcc_order_line_val_t* order_line_val =
        (tpcc_order_line_val_t*)ol_obj->value;

    order_line_val->ol_i_id = int32_t(ol_i_id);
    order_line_val->ol_delivery_d = 0;  // not delivered yet
    order_line_val->ol_amount = float(ol_quantity) * item_val->i_price;
    order_line_val->ol_supply_w_id = int32_t(local_supplies[ol_number - 1]);
    order_line_val->ol_quantity = int8_t(ol_quantity);
    order_line_val->debug_magic = tpcc_add_magic;
  }

  for (int ol_number = 1; ol_number <= num_remote_stocks; ol_number++) {
    const int64_t ol_i_id = remote_item_ids[ol_number - 1];
    const uint32_t ol_quantity =
        tpcc_client->RandomNumber(tpcc_client->r, 1, 10);
    // read item info
    tpcc_item_key_t tpcc_item_key;
    tpcc_item_key.i_id = ol_i_id;

    auto item_obj = std::make_shared<DataItem>(
        (table_id_t)TPCCTableType::kItemTable, tpcc_item_key.item_key);
    dtx->AddToReadOnlySet(item_obj);

    int64_t s_key = remote_stocks[ol_number - 1];
    // read and update stock info
    tpcc_stock_key_t stock_key;
    stock_key.s_id = s_key;

    auto stock_obj = std::make_shared<DataItem>(
        (table_id_t)TPCCTableType::kStockTable, stock_key.item_key);
    dtx->AddToReadWriteSet(stock_obj);

    if (!dtx->TxExe()) return false;

    tpcc_item_val_t* item_val = (tpcc_item_val_t*)item_obj->value;
    tpcc_stock_val_t* stock_val = (tpcc_stock_val_t*)stock_obj->value;

    if (item_val->debug_magic != tpcc_add_magic) {
      // SDS_PERROR << "[FATAL] Read item unmatch, tid-cid-txid: " << dtx->t_id
      //            << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (stock_val->debug_magic != tpcc_add_magic) {
      // SDS_PERROR << "[FATAL] Read stock unmatch, tid-cid-txid: " << dtx->t_id
      //            << "-" << dtx->coro_id << "-" << tx_id;
    }

    if (stock_val->s_quantity - ol_quantity >= 10) {
      stock_val->s_quantity -= ol_quantity;
    } else {
      stock_val->s_quantity += (-int32_t(ol_quantity) + 91);
    }

    stock_val->s_ytd += ol_quantity;
    stock_val->s_remote_cnt +=
        (remote_supplies[ol_number - 1] == warehouse_id) ? 0 : 1;

    // insert order line record
    int64_t ol_key = tpcc_client->MakeOrderLineKey(
        warehouse_id, district_id, my_next_o_id, num_local_stocks + ol_number);
    tpcc_order_line_key_t order_line_key;
    order_line_key.ol_id = ol_key;

    auto ol_obj = std::make_shared<DataItem>(
        (table_id_t)TPCCTableType::kOrderLineTable,
        sizeof(tpcc_order_line_val_t), order_line_key.item_key, tx_id, 1);
    dtx->AddToReadWriteSet(ol_obj);
    if (!dtx->TxExe()) return false;

    tpcc_order_line_val_t* order_line_val =
        (tpcc_order_line_val_t*)ol_obj->value;

    order_line_val->ol_i_id = int32_t(ol_i_id);
    order_line_val->ol_delivery_d = 0;  // not delivered yet
    order_line_val->ol_amount = float(ol_quantity) * item_val->i_price;
    order_line_val->ol_supply_w_id = int32_t(remote_supplies[ol_number - 1]);
    order_line_val->ol_quantity = int8_t(ol_quantity);
    order_line_val->debug_magic = tpcc_add_magic;
  }

  bool commit_status = dtx->TxCommit();
  return commit_status;
}

bool TxPayment(tx_id_t tx_id, DTX* dtx) {
  /*
   "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE,
   W_ZIP FROM WAREHOUSE WHERE W_ID = ?", # w_id "updateWarehouseBalance":
   "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?", # h_amount, w_id
   "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
   FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", # w_id, d_id
   "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID
   = ? AND D_ID = ?", # h_amount, d_w_id, d_id "getCustomerByCustomerId":
   "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY,
   C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
   C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID =
   ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id "getCustomersByLastName":
   "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY,
   C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
   C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID =
   ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
   "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?,
   C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
   # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
   "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?,
   C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance,
   c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id "insertHistory": "INSERT
   INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
   */

  dtx->TxBegin(tx_id);

  // Generate parameters

  int x = tpcc_client->RandomNumber(tpcc_client->r, 1, 100);
  int y = tpcc_client->RandomNumber(tpcc_client->r, 1, 100);

  int warehouse_id_start_ = 1;
  int warehouse_id_end_ = tpcc_client->num_warehouse;

  int district_id_start = 1;
  int district_id_end_ = tpcc_client->num_district_per_warehouse;

  const uint32_t warehouse_id = tpcc_client->PickWarehouseId(
      tpcc_client->r, warehouse_id_start_, warehouse_id_end_);
  const uint32_t district_id = tpcc_client->RandomNumber(
      tpcc_client->r, district_id_start, district_id_end_);

  int32_t c_w_id;
  int32_t c_d_id;
  if (tpcc_client->num_warehouse == 1 || x <= 85) {
    // 85%: paying through own warehouse (or there is only 1 warehouse)
    c_w_id = warehouse_id;
    c_d_id = district_id;
  } else {
    // 15%: paying through another warehouse:
    // select in range [1, num_warehouses] excluding w_id
    do {
      c_w_id = tpcc_client->RandomNumber(tpcc_client->r, 1,
                                         tpcc_client->num_warehouse);
    } while (c_w_id == warehouse_id);
    c_d_id = tpcc_client->RandomNumber(tpcc_client->r, district_id_start,
                                       district_id_end_);
  }
  uint32_t customer_id = 0;
  // The payment amount (H_AMOUNT) is randomly selected within [1.00 ..
  // 5,000.00].
  float h_amount =
      (float)tpcc_client->RandomNumber(tpcc_client->r, 100, 500000) / 100.0;
  if (y <= 60) {
    // 60%: payment by last name
    char last_name[tpcc_customer_val_t::MAX_LAST + 1];
    size_t size =
        (tpcc_client->GetNonUniformCustomerLastNameLoad(tpcc_client->r)).size();
    assert(tpcc_customer_val_t::MAX_LAST - size >= 0);
    strcpy(
        last_name,
        tpcc_client->GetNonUniformCustomerLastNameLoad(tpcc_client->r).c_str());
    // FIXME:: Find customer by the last name
    // All rows in the CUSTOMER table with matching C_W_ID, C_D_ID and C_LAST
    // are selected sorted by C_FIRST in ascending order. Let n be the number of
    // rows selected. C_ID, C_FIRST, C_MIDDLE, C_STREET_1, C_STREET_2, C_CITY,
    // C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, and
    // C_BALANCE are retrieved from the row at position (n/ 2 rounded up to the
    // next integer) in the sorted set of selected rows from the CUSTOMER table.
    customer_id = tpcc_client->GetCustomerId(tpcc_client->r);
  } else {
    // 40%: payment by id
    assert(y > 60);
    customer_id = tpcc_client->GetCustomerId(tpcc_client->r);
  }

  // Run

  tpcc_warehouse_key_t ware_key;
  ware_key.w_id = warehouse_id;
  auto ware_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kWarehouseTable, ware_key.item_key);
  dtx->AddToReadWriteSet(ware_obj);

  uint64_t d_key = tpcc_client->MakeDistrictKey(warehouse_id, district_id);
  tpcc_district_key_t dist_key;
  dist_key.d_id = d_key;
  auto dist_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kDistrictTable, dist_key.item_key);
  dtx->AddToReadWriteSet(dist_obj);

  tpcc_customer_key_t cust_key;
  cust_key.c_id = tpcc_client->MakeCustomerKey(c_w_id, c_d_id, customer_id);
  auto cust_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kCustomerTable, cust_key.item_key);
  dtx->AddToReadWriteSet(cust_obj);

  tpcc_history_key_t hist_key;
  hist_key.h_id = tpcc_client->MakeHistoryKey(warehouse_id, district_id, c_w_id,
                                              c_d_id, customer_id);
  auto hist_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kHistoryTable, sizeof(tpcc_history_val_t),
      hist_key.item_key, tx_id, 1);
  dtx->AddToReadWriteSet(hist_obj);

  if (!dtx->TxExe()) return false;

  tpcc_warehouse_val_t* ware_val = (tpcc_warehouse_val_t*)ware_obj->value;
  std::string check(ware_val->w_zip);
  if (check != tpcc_zip_magic) {
    // SDS_PERROR << "[FATAL] Read warehouse unmatch, tid-cid-txid: " <<
    // dtx->t_id
    //            << "-" << dtx->coro_id << "-" << tx_id;
  }

  tpcc_district_val_t* dist_val = (tpcc_district_val_t*)dist_obj->value;
  check = std::string(dist_val->d_zip);
  if (check != tpcc_zip_magic) {
    // SDS_PERROR << "[FATAL] Read district unmatch, tid-cid-txid: " <<
    // dtx->t_id
    //            << "-" << dtx->coro_id << "-" << tx_id;
  }

  tpcc_customer_val_t* cust_val = (tpcc_customer_val_t*)cust_obj->value;
  // c_since never be 0
  if (cust_val->c_since == 0) {
    // SDS_PERROR << "[FATAL] Read customer unmatch, tid-cid-txid: " <<
    // dtx->t_id
    //            << "-" << dtx->coro_id << "-" << tx_id;
  }

  ware_val->w_ytd += h_amount;
  dist_val->d_ytd += h_amount;

  cust_val->c_balance -= h_amount;
  cust_val->c_ytd_payment += h_amount;
  cust_val->c_payment_cnt += 1;

  if (strcmp(cust_val->c_credit, BAD_CREDIT) == 0) {
    // Bad credit: insert history into c_data
    static const int HISTORY_SIZE = tpcc_customer_val_t::MAX_DATA + 1;
    char history[HISTORY_SIZE];
    int characters = snprintf(
        history, HISTORY_SIZE, "(%d, %d, %d, %d, %d, %.2f)\n", customer_id,
        c_d_id, c_w_id, district_id, warehouse_id, h_amount);
    assert(characters < HISTORY_SIZE);

    // Perform the insert with a move and copy
    int current_keep = static_cast<int>(strlen(cust_val->c_data));
    if (current_keep + characters > tpcc_customer_val_t::MAX_DATA) {
      current_keep = tpcc_customer_val_t::MAX_DATA - characters;
    }
    assert(current_keep + characters <= tpcc_customer_val_t::MAX_DATA);
    memmove(cust_val->c_data + characters, cust_val->c_data, current_keep);
    memcpy(cust_val->c_data, history, characters);
    cust_val->c_data[characters + current_keep] = '\0';
    assert(strlen(cust_val->c_data) == characters + current_keep);
  }

  tpcc_history_val_t* hist_val = (tpcc_history_val_t*)hist_obj->value;

  hist_val->h_date =
      tpcc_client->GetCurrentTimeMillis();  // different time at server and
                                            // client cause errors?
  hist_val->h_amount = h_amount;
  strcpy(hist_val->h_data, ware_val->w_name);
  strcat(hist_val->h_data, "    ");
  strcat(hist_val->h_data, dist_val->d_name);

  bool commit_status = dtx->TxCommit();
  return commit_status;
}

bool TxDelivery(tx_id_t tx_id, DTX* dtx) {
  /*
  "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID =
  ? AND NO_O_ID > -1 LIMIT 1", # "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE
  NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", # d_id, w_id, no_o_id "getCId":
  "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", #
  no_o_id, d_id, w_id "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE
  O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
  "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ?
  AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
  "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND
  OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id "updateCustomer": "UPDATE
  CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND
  C_W_ID = ?", # ol_total, c_id, d_id, w_id
  */

  dtx->TxBegin(tx_id);

  // Generate parameters

  int warehouse_id_start_ = 1;
  int warehouse_id_end_ = tpcc_client->num_warehouse;
  const uint32_t warehouse_id = tpcc_client->PickWarehouseId(
      tpcc_client->r, warehouse_id_start_, warehouse_id_end_);
  const int o_carrier_id = tpcc_client->RandomNumber(
      tpcc_client->r, tpcc_order_val_t::MIN_CARRIER_ID,
      tpcc_order_val_t::MAX_CARRIER_ID);
  const uint32_t current_ts = tpcc_client->GetCurrentTimeMillis();

  for (int d_id = 1; d_id <= tpcc_client->num_district_per_warehouse; d_id++) {
    // FIXME: select the lowest NO_O_ID with matching NO_W_ID (equals W_ID) and
    // NO_D_ID (equals D_ID) in the NEW-ORDER table
    int min_o_id =
        tpcc_client->num_customer_per_district *
            tpcc_new_order_val_t::SCALE_CONSTANT_BETWEEN_NEWORDER_ORDER +
        1;
    int max_o_id = tpcc_client->num_customer_per_district;
    int o_id = tpcc_client->RandomNumber(tpcc_client->r, min_o_id, max_o_id);

    int64_t no_key = tpcc_client->MakeNewOrderKey(warehouse_id, d_id, o_id);
    tpcc_new_order_key_t norder_key;
    norder_key.no_id = no_key;
    auto norder_obj = std::make_shared<DataItem>(
        (table_id_t)TPCCTableType::kNewOrderTable, norder_key.item_key);
    dtx->AddToReadOnlySet(norder_obj);

    // Get the new order record with the o_id. Probe if the new order record
    // exists
    if (!dtx->TxExe()) {
      dtx->RemoveLastROItem();
      continue;
    }

    // The new order record exists. Remove the new order obj from read only set
    dtx->RemoveLastROItem();

    // Add the new order obj to read write set to be deleted
    dtx->AddToReadWriteSet(norder_obj);

    uint64_t o_key = tpcc_client->MakeOrderKey(warehouse_id, d_id, o_id);
    tpcc_order_key_t order_key;
    order_key.o_id = o_key;
    auto order_obj = std::make_shared<DataItem>(
        (table_id_t)TPCCTableType::kOrderTable, order_key.item_key);
    dtx->AddToReadWriteSet(order_obj);

    // The row in the ORDER table with matching O_W_ID (equals W_ ID), O_D_ID
    // (equals D_ID), and O_ID (equals NO_O_ID) is selected
    if (!dtx->TxExe()) return false;

    auto* no_val = (tpcc_new_order_val_t*)norder_obj->value;
    if (no_val->debug_magic != tpcc_add_magic) {
      // SDS_PERROR << "[FATAL] Read new order unmatch, tid-cid-txid: "
      //            << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }

    norder_obj->valid = 0;  // deleteNewOrder

    // o_entry_d never be 0
    tpcc_order_val_t* order_val = (tpcc_order_val_t*)order_obj->value;
    if (order_val->o_entry_d == 0) {
      // SDS_PERROR << "[FATAL] Read order unmatch, tid-cid-txid: " << dtx->t_id
      //            << "-" << dtx->coro_id << "-" << tx_id;
    }

    // O_C_ID, the customer number, is retrieved
    int32_t customer_id = order_val->o_c_id;

    // O_CARRIER_ID is updated
    order_val->o_carrier_id = o_carrier_id;

    // All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID),
    // OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are selected. All
    // OL_DELIVERY_D, the delivery dates, are updated to the current system time
    // The sum of all OL_AMOUNT is retrieved
    float sum_ol_amount = 0;
    for (int line_number = 1; line_number <= tpcc_order_line_val_t::MAX_OL_CNT;
         ++line_number) {
      int64_t ol_key =
          tpcc_client->MakeOrderLineKey(warehouse_id, d_id, o_id, line_number);
      tpcc_order_line_key_t order_line_key;
      order_line_key.ol_id = ol_key;
      auto ol_obj = std::make_shared<DataItem>(
          (table_id_t)TPCCTableType::kOrderLineTable, order_line_key.item_key);
      dtx->AddToReadOnlySet(ol_obj);

      if (!dtx->TxExe()) {
        // Fail not abort
        dtx->RemoveLastROItem();
        continue;
      }
      tpcc_order_line_val_t* order_line_val =
          (tpcc_order_line_val_t*)ol_obj->value;
      if (order_line_val->debug_magic != tpcc_add_magic) {
        // SDS_PERROR << "[FATAL] Read order line unmatch, tid-cid-txid: "
        //            << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
      }
      order_line_val->ol_delivery_d = current_ts;
      sum_ol_amount += order_line_val->ol_amount;
    }

    // The row in the CUSTOMER table with matching C_W_ID (equals W_ID), C_D_ID
    // (equals D_ID), and C_ID (equals O_C_ID) is selected
    tpcc_customer_key_t cust_key;
    cust_key.c_id =
        tpcc_client->MakeCustomerKey(warehouse_id, d_id, customer_id);
    auto cust_obj = std::make_shared<DataItem>(
        (table_id_t)TPCCTableType::kCustomerTable, cust_key.item_key);
    dtx->AddToReadWriteSet(cust_obj);

    if (!dtx->TxExe()) return false;

    tpcc_customer_val_t* cust_val = (tpcc_customer_val_t*)cust_obj->value;
    // c_since never be 0
    if (cust_val->c_since == 0) {
      // SDS_PERROR << "[FATAL] Read customer unmatch, tid-cid-txid: " <<
      // dtx->t_id
      //            << "-" << dtx->coro_id << "-" << tx_id;
    }

    // C_BALANCE is increased by the sum of all order-line amounts (OL_AMOUNT)
    // previously retrieved
    cust_val->c_balance += sum_ol_amount;

    // C_DELIVERY_CNT is incremented by 1
    cust_val->c_delivery_cnt += 1;
  }

  bool commit_status = dtx->TxCommit();
  return commit_status;
}

bool TxOrderStatus(tx_id_t tx_id, DTX* dtx) {
  /*
  "ORDER_STATUS": {
  "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE
  FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id,
  c_id "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST,
  C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER
  BY C_FIRST", # w_id, d_id, c_last "getLastOrder": "SELECT O_ID, O_CARRIER_ID,
  O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY
  O_ID DESC LIMIT 1", # w_id, d_id, c_id "getOrderLines": "SELECT
  OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE
  WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id
  },
  */

  dtx->TxBegin(tx_id);

  int y = tpcc_client->RandomNumber(tpcc_client->r, 1, 100);

  int warehouse_id_start_ = 1;
  int warehouse_id_end_ = tpcc_client->num_warehouse;

  int district_id_start = 1;
  int district_id_end_ = tpcc_client->num_district_per_warehouse;

  const uint32_t warehouse_id = tpcc_client->PickWarehouseId(
      tpcc_client->r, warehouse_id_start_, warehouse_id_end_);
  const uint32_t district_id = tpcc_client->RandomNumber(
      tpcc_client->r, district_id_start, district_id_end_);
  uint32_t customer_id = 0;

  if (y <= 60) {
    // FIXME:: Find customer by the last name
    customer_id = tpcc_client->GetCustomerId(tpcc_client->r);
  } else {
    customer_id = tpcc_client->GetCustomerId(tpcc_client->r);
  }

  tpcc_customer_key_t cust_key;
  cust_key.c_id =
      tpcc_client->MakeCustomerKey(warehouse_id, district_id, customer_id);
  auto cust_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kCustomerTable, cust_key.item_key);
  dtx->AddToReadOnlySet(cust_obj);

  // FIXME: Currently, we use a random order_id to maintain the distributed
  // transaction payload, but need to search the largest o_id by o_w_id, o_d_id
  // and o_c_id from the order table
  int32_t order_id = tpcc_client->RandomNumber(
      tpcc_client->r, 1, tpcc_client->num_customer_per_district);
  uint64_t o_key =
      tpcc_client->MakeOrderKey(warehouse_id, district_id, order_id);
  tpcc_order_key_t order_key;
  order_key.o_id = o_key;
  auto order_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kOrderTable, order_key.item_key);
  dtx->AddToReadOnlySet(order_obj);

  if (!dtx->TxExe()) return false;

  tpcc_customer_val_t* cust_val = (tpcc_customer_val_t*)cust_obj->value;
  // c_since never be 0
  if (cust_val->c_since == 0) {
    // SDS_PERROR << "[FATAL] Read customer unmatch, tid-cid-txid: " <<
    // dtx->t_id
    //            << "-" << dtx->coro_id << "-" << tx_id;
  }

  // o_entry_d never be 0
  tpcc_order_val_t* order_val = (tpcc_order_val_t*)order_obj->value;
  if (order_val->o_entry_d == 0) {
    // SDS_PERROR << "[FATAL] Read order unmatch, tid-cid-txid: " << dtx->t_id
    //            << "-" << dtx->coro_id << "-" << tx_id;
  }
  for (int i = 1; i <= order_val->o_ol_cnt; i++) {
    int64_t ol_key =
        tpcc_client->MakeOrderLineKey(warehouse_id, district_id, order_id, i);
    tpcc_order_line_key_t order_line_key;
    order_line_key.ol_id = ol_key;
    auto ol_obj = std::make_shared<DataItem>(
        (table_id_t)TPCCTableType::kOrderLineTable, order_line_key.item_key);
    dtx->AddToReadOnlySet(ol_obj);
  }
  if (!dtx->TxExe()) return false;

  // SDS_INFO("find order line %ld", tx_id);
  bool commit_status = dtx->TxCommit();

  // SDS_INFO("order commit %d %ld", commit_status, tx_id);
  return commit_status;
}

bool TxStockLevel(tx_id_t tx_id, DTX* dtx) {
  /*
   "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
   "getStockCount": "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
   WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID
   = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?
   */

  dtx->TxBegin(tx_id);

  int32_t threshold = tpcc_client->RandomNumber(
      tpcc_client->r, tpcc_stock_val_t::MIN_STOCK_LEVEL_THRESHOLD,
      tpcc_stock_val_t::MAX_STOCK_LEVEL_THRESHOLD);

  int warehouse_id_start_ = 1;
  int warehouse_id_end_ = tpcc_client->num_warehouse;

  int district_id_start = 1;
  int district_id_end_ = tpcc_client->num_district_per_warehouse;

  const uint32_t warehouse_id = tpcc_client->PickWarehouseId(
      tpcc_client->r, warehouse_id_start_, warehouse_id_end_);
  const uint32_t district_id = tpcc_client->RandomNumber(
      tpcc_client->r, district_id_start, district_id_end_);

  uint64_t d_key = tpcc_client->MakeDistrictKey(warehouse_id, district_id);
  tpcc_district_key_t dist_key;
  dist_key.d_id = d_key;
  auto dist_obj = std::make_shared<DataItem>(
      (table_id_t)TPCCTableType::kDistrictTable, dist_key.item_key);
  dtx->AddToReadOnlySet(dist_obj);

  if (!dtx->TxExe()) return false;
  // SDS_INFO("get district done, read only set size = %d",
  //          dtx->read_only_set.size());
  tpcc_district_val_t* dist_val = (tpcc_district_val_t*)dist_obj->value;
  std::string check = std::string(dist_val->d_zip);
  if (check != tpcc_zip_magic) {
    // SDS_PERROR << "[FATAL] Read district unmatch, tid-cid-txid: " <<
    // dtx->t_id
    //            << "-" << dtx->coro_id << "-" << tx_id;
  }

  int32_t o_id = dist_val->d_next_o_id;

  std::vector<int32_t> s_i_ids;
  s_i_ids.reserve(300);

  // Iterate over [o_id-20, o_id)
  for (int order_id = o_id - tpcc_stock_val_t::STOCK_LEVEL_ORDERS;
       order_id < o_id; ++order_id) {
    // Populate line_numer is random: [Min_OL_CNT, MAX_OL_CNT)
    for (int line_number = 1; line_number <= tpcc_order_line_val_t::MAX_OL_CNT;
         ++line_number) {
      int64_t ol_key = tpcc_client->MakeOrderLineKey(warehouse_id, district_id,
                                                     order_id, line_number);
      tpcc_order_line_key_t order_line_key;
      order_line_key.ol_id = ol_key;
      auto ol_obj = std::make_shared<DataItem>(
          (table_id_t)TPCCTableType::kOrderLineTable, order_line_key.item_key);
      dtx->AddToReadOnlySet(ol_obj);

      if (!dtx->TxExe()) {
        // Not found, not abort
        dtx->RemoveLastROItem();
        break;
      }

      tpcc_order_line_val_t* ol_val = (tpcc_order_line_val_t*)ol_obj->value;
      if (ol_val->debug_magic != tpcc_add_magic) {
        // SDS_PERROR << "[FATAL] Read order line unmatch, tid-cid-txid: "
        //            << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
      }

      int64_t s_key = tpcc_client->MakeStockKey(warehouse_id, ol_val->ol_i_id);
      tpcc_stock_key_t stock_key;
      stock_key.s_id = s_key;
      auto stock_obj = std::make_shared<DataItem>(
          (table_id_t)TPCCTableType::kStockTable, stock_key.item_key);
      dtx->AddToReadOnlySet(stock_obj);

      if (!dtx->TxExe()) return false;

      tpcc_stock_val_t* stock_val = (tpcc_stock_val_t*)stock_obj->value;
      if (stock_val->debug_magic != tpcc_add_magic) {
        // SDS_PERROR << "[FATAL] Read stock unmatch, tid-cid-txid: " <<
        // dtx->t_id
        //            << "-" << dtx->coro_id << "-" << tx_id;
      }

      if (stock_val->s_quantity < threshold) {
        s_i_ids.push_back(ol_val->ol_i_id);
      }
    }
  }

  // Filter out duplicate s_i_id: multiple order lines can have the same item
  // In O3, this code may be optimized since num_distinct is not outputed.
  std::sort(s_i_ids.begin(), s_i_ids.end());
  int num_distinct = 0;  // The output of this transaction
  int32_t last = -1;     // -1 is an invalid s_i_id
  for (size_t i = 0; i < s_i_ids.size(); ++i) {
    if (s_i_ids[i] != last) {
      last = s_i_ids[i];
      num_distinct += 1;
    }
  }

  bool commit_status = dtx->TxCommit();
  return commit_status;
}

/******************** The business logic (Transaction) end ********************/

void WarmUp(DTXContext* context) {
  DTX* dtx = new DTX(context, txn_sys, lease, delayed, offset);
  bool tx_committed = false;
  for (int i = 0; i < 5000; ++i) {
    TPCCTxType tx_type = workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_local;
    // Global atomic transaction id
    // SDS_INFO("txid %ld", iter);
    switch (tx_type) {
      case TPCCTxType::kNewOrder:
        // SDS_INFO("new order");
        TxNewOrder(iter, dtx);
        break;
      case TPCCTxType::kDelivery:
        // SDS_INFO("delivery");
        TxDelivery(iter, dtx);
        break;
      case TPCCTxType::kOrderStatus:
        // SDS_INFO("order status");
        TxOrderStatus(iter, dtx);
        break;
      case TPCCTxType::kPayment:
        // SDS_INFO("payment");
        TxPayment(iter, dtx);
        break;
      case TPCCTxType::kStockLevel:
        // SDS_INFO("stock level");
        TxStockLevel(iter, dtx);
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

void RunTx(DTXContext* context) {
  DTX* dtx = new DTX(context, txn_sys, lease, delayed, offset);
  struct timespec tx_start_time, tx_end_time;
  bool tx_committed = false;
  uint64_t attempt_tx = 0;
  uint64_t commit_tx = 0;
  tx_id_local = (uint64_t)GetThreadID() << 50;

  int timer_idx = GetThreadID() * coroutines + GetTaskID();
  // Running transactions
  while (true) {
    uint64_t iter = ++tx_id_local;  // Global atomic transaction id
    attempt_tx++;

    TPCCTxType tx_type = workgen_arr[iter % 100];
    clock_gettime(CLOCK_REALTIME, &tx_start_time);
    for (int i = 0; i < 20; i++) {
      switch (tx_type) {
        case TPCCTxType::kNewOrder:
          tx_committed = TxNewOrder(iter, dtx);
          break;
        case TPCCTxType::kDelivery:
          tx_committed = TxDelivery(iter, dtx);
          break;
        case TPCCTxType::kOrderStatus:
          tx_committed = TxOrderStatus(iter, dtx);
          break;
        case TPCCTxType::kPayment:
          tx_committed = TxPayment(iter, dtx);
          break;
        case TPCCTxType::kStockLevel:
          tx_committed = TxStockLevel(iter, dtx);
          break;
        default:
          printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
          abort();
      }
      if (tx_committed) {
        break;
      }
    }

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

void execute_thread(int id, DTXContext* context) {
  BindCore(id);

  ATTEMPTED_NUM = kMaxTransactions / threads / coroutines;
  auto hostname = GetHostName();
  seed = MurmurHash3_x86_32(hostname.c_str(), hostname.length(), 0xcc9e2d51) *
             kMaxThreads +
         id;
  tpcc_client = new TPCC(seed);
  workgen_arr = tpcc_client->CreateWorkgenArray();

  SDS_INFO("warm up start %d", id);
  WarmUp(context);
  SDS_INFO("warm up done %d", id);
  TaskPool::Enable();
  auto& task_pool = TaskPool::Get();
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

void synchronize_begin(DTXContext* ctx) {
  if (getenv("COMPUTE_NODES")) {
    int nr_compute_nodes = (int)atoi(getenv("COMPUTE_NODES"));
    if (nr_compute_nodes <= 1) return;
    GlobalAddress addr(0, sizeof(SuperChunk));
    uint64_t* buf = (uint64_t*)ctx->Alloc(8);
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

void synchronize_end(DTXContext* ctx) {
  if (getenv("COMPUTE_NODES")) {
    uint64_t* buf = (uint64_t*)ctx->Alloc(8);
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

void report(double elapsed_time, JsonConfig& config) {
  assert(commits.load() <= kMaxTransactions);
  std::sort(timer, timer + commits.load());
  std::string dump_prefix;
  if (getenv("DUMP_PREFIX")) {
    dump_prefix = std::string(getenv("DUMP_PREFIX"));
  } else {
    dump_prefix = "dtx-tpcc";
  }
  SDS_INFO(
      "%s: #thread = %ld, #coro_per_thread = %ld, "
      "attempt txn = %.3lf M/s, committed txn = %.3lf k/s, "
      "P50 latency = %.3lf us, P99 latency = %.3lf us, abort rate = %.3lf, "
      "RDMA ops per txn = %.3lf M, RDMA ops per second = %.3lf M",
      dump_prefix.c_str(), threads, coroutines, attempts.load() / elapsed_time,
      commits.load() * 1000.0 / elapsed_time,
      timer[(int)(0.5 * commits.load())], timer[(int)(0.99 * commits.load())],
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
  FILE* fout = fopen(dump_file_path.c_str(), "a+");
  if (!fout) {
    // SDS_PERROR("fopen");
    SDS_INFO("fopen");
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

int main(int argc, char** argv) {
  BindCore(1);
  const char* path = ROOT_DIR "/config/transaction.json";

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
  delayed = config.get("delayed").get_bool();
  offset = config.get("offset").get_double();
  if (txn_sys == DTX_SYS::OOCC) {
    SDS_INFO("running OOCC");
  } else if (txn_sys == DTX_SYS::OCC) {
    SDS_INFO("running OCC");
  }
  srand48(time(nullptr));
  threads = argc < 2 ? 1 : atoi(argv[1]);
  coroutines = argc < 3 ? 1 : atoi(argv[2]);
  timer = new double[kMaxTransactions];
  DTXContext* context = new DTXContext(config, threads);
  tpcc_client = new TPCC(seed);
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