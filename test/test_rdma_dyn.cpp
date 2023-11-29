/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2022-2023 Feng Ren, Tsinghua University
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <immintrin.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <iostream>
#include <random>

using namespace std;
// | max x| max s| n x| n s|  lock representation
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

const uint64_t COUNT_MAX = 32768;

uint64_t reset_write_lock(uint64_t maxs) {
  uint64_t t = COUNT_MAX << 16;
  t += maxs;
  return (t << 32) + t;
}
// 01011100111101111000000000000000
// 01011100111101111000000000000000
uint64_t reset_read_lock(uint64_t maxx) {
  uint64_t lock = maxx << 16;
  for (int i = 63; i >= 0; i--) {
    cout << ((lock >> i) & 1);
  }
  cout << endl;
  for (int i = 63; i >= 0; i--) {
    cout << ((COUNT_MAX >> i) & 1);
  }
  cout << endl;
  lock += COUNT_MAX;
  for (int i = 63; i >= 0; i--) {
    cout << ((lock >> i) & 1);
  }
  cout << endl;
  uint64_t result = lock << 32;
  return result + lock;
}
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

int main(int argc, char **argv) {
  uint64_t t = 0;
  auto re = reset_read_lock(23799);
  for (int i = 63; i >= 0; i--) {
    cout << ((re >> i) & 1);
  }
  cout << endl;
  // cout << get_ns(re) << "  " << get_max_s(re) << endl;

  return 0;
}
