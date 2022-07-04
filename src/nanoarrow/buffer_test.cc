// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cerrno>
#include <cstring>
#include <string>

#include <gtest/gtest.h>

#include "nanoarrow/nanoarrow.h"

// This test allocator guarantees that allocator->reallocate will return
// a new pointer so that we can test when reallocations happen whilst
// building buffers.
static uint8_t* TestAllocatorAllocate(struct ArrowBufferAllocator* allocator,
                                      int64_t size) {
  return reinterpret_cast<uint8_t*>(malloc(size));
}

static uint8_t* TestAllocatorReallocate(struct ArrowBufferAllocator* allocator,
                                        uint8_t* ptr, int64_t old_size,
                                        int64_t new_size) {
  uint8_t* new_ptr = TestAllocatorAllocate(allocator, new_size);

  int64_t copy_size = std::min<int64_t>(old_size, new_size);
  if (new_ptr != nullptr && copy_size > 0) {
    memcpy(new_ptr, ptr, copy_size);
  }

  if (ptr != nullptr) {
    free(ptr);
  }

  return new_ptr;
}

static void TestAllocatorFree(struct ArrowBufferAllocator* allocator, uint8_t* ptr,
                              int64_t size) {
  free(ptr);
}

static struct ArrowBufferAllocator test_allocator = {
    &TestAllocatorAllocate, &TestAllocatorReallocate, &TestAllocatorFree, nullptr};

TEST(BufferTest, BufferTestBasic) {
  struct ArrowBuffer buffer;

  // Init
  ArrowBufferInit(&buffer);
  buffer.allocator = &test_allocator;
  EXPECT_EQ(buffer.data, nullptr);
  EXPECT_EQ(buffer.capacity_bytes, 0);
  EXPECT_EQ(buffer.size_bytes, 0);

  // Reserve where capacity > current_capacity * growth_factor
  EXPECT_EQ(ArrowBufferReserveAdditional(&buffer, 10), NANOARROW_OK);
  EXPECT_NE(buffer.data, nullptr);
  EXPECT_EQ(buffer.capacity_bytes, 10);
  EXPECT_EQ(buffer.size_bytes, 0);

  // Write without triggering a realloc
  uint8_t* first_data = buffer.data;
  EXPECT_EQ(ArrowBufferWriteChecked(&buffer, "1234567890", 10), NANOARROW_OK);
  EXPECT_EQ(buffer.data, first_data);
  EXPECT_EQ(buffer.capacity_bytes, 10);
  EXPECT_EQ(buffer.size_bytes, 10);

  // Write triggering a realloc
  EXPECT_EQ(ArrowBufferWriteChecked(&buffer, "1", 2), NANOARROW_OK);
  EXPECT_NE(buffer.data, first_data);
  EXPECT_EQ(buffer.capacity_bytes, 22);
  EXPECT_EQ(buffer.size_bytes, 12);
  EXPECT_STREQ(reinterpret_cast<char*>(buffer.data), "12345678901");

  // Shrink capacity
  EXPECT_EQ(ArrowBufferReallocate(&buffer, 5), NANOARROW_OK);
  EXPECT_EQ(buffer.capacity_bytes, 5);
  EXPECT_EQ(buffer.size_bytes, 5);
  EXPECT_EQ(strncmp(reinterpret_cast<char*>(buffer.data), "12345", 5), 0);

  // Transfer responsibility to the same allocator
  first_data = buffer.data;
  EXPECT_EQ(ArrowBufferSetAllocator(&buffer, buffer.allocator), NANOARROW_OK);
  EXPECT_EQ(buffer.data, first_data);
  EXPECT_EQ(buffer.capacity_bytes, 5);
  EXPECT_EQ(buffer.size_bytes, 5);
  EXPECT_EQ(strncmp(reinterpret_cast<char*>(buffer.data), "12345", 5), 0);

  // Transfer responsibility to another allocator
  struct ArrowBufferAllocator non_default_allocator;
  memcpy(&non_default_allocator, ArrowBufferAllocatorDefault(),
         sizeof(struct ArrowBufferAllocator));

  EXPECT_EQ(ArrowBufferSetAllocator(&buffer, &non_default_allocator), NANOARROW_OK);
  EXPECT_NE(buffer.data, first_data);
  EXPECT_EQ(buffer.capacity_bytes, 5);
  EXPECT_EQ(buffer.size_bytes, 5);
  EXPECT_EQ(strncmp(reinterpret_cast<char*>(buffer.data), "12345", 5), 0);

  // Free the buffer
  ArrowBufferRelease(&buffer);
  EXPECT_EQ(buffer.data, nullptr);
  EXPECT_EQ(buffer.capacity_bytes, 0);
  EXPECT_EQ(buffer.size_bytes, 0);

  // Transfer allocator with empty buffer
  EXPECT_EQ(ArrowBufferSetAllocator(&buffer, ArrowBufferAllocatorDefault()),
            NANOARROW_OK);
  EXPECT_EQ(buffer.data, nullptr);
  EXPECT_EQ(buffer.capacity_bytes, 0);
  EXPECT_EQ(buffer.size_bytes, 0);
}

TEST(BufferTest, BufferTestError) {
  struct ArrowBuffer buffer;
  ArrowBufferInit(&buffer);
  EXPECT_EQ(ArrowBufferReallocate(&buffer, std::numeric_limits<int64_t>::max()), ENOMEM);
  EXPECT_EQ(
      ArrowBufferWriteChecked(&buffer, nullptr, std::numeric_limits<int64_t>::max()),
      ENOMEM);

  ASSERT_EQ(ArrowBufferWriteChecked(&buffer, "abcd", 4), NANOARROW_OK);
  struct ArrowBufferAllocator non_default_allocator;
  memcpy(&non_default_allocator, ArrowBufferAllocatorDefault(),
         sizeof(struct ArrowBufferAllocator));
  buffer.capacity_bytes = std::numeric_limits<int64_t>::max();

  EXPECT_EQ(ArrowBufferSetAllocator(&buffer, &non_default_allocator), ENOMEM);
}
