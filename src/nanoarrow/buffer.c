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

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "nanoarrow.h"

ArrowErrorCode ArrowBufferInit(struct ArrowBuffer* buffer) {
  buffer->data = NULL;
  buffer->size_bytes = 0;
  buffer->capacity_bytes = 0;
  buffer->allocator = ArrowBufferAllocatorDefault();
  return NANOARROW_OK;
}

ArrowErrorCode ArrowBufferSetAllocator(struct ArrowBuffer* buffer,
                                       struct ArrowBufferAllocator* allocator) {
  if (buffer->allocator == allocator) {
    return NANOARROW_OK;
  }

  if (buffer->data == NULL) {
    buffer->allocator = allocator;
    return NANOARROW_OK;
  }

  uint8_t* data2 = allocator->allocate(allocator, buffer->capacity_bytes);
  if (data2 == NULL) {
    return ENOMEM;
  }

  if (buffer->size_bytes > 0) {
    memcpy(data2, buffer->data, buffer->size_bytes);
  }

  ArrowBufferRelease(buffer);

  buffer->data = data2;
  buffer->allocator = allocator;
  return NANOARROW_OK;
}

void ArrowBufferRelease(struct ArrowBuffer* buffer) {
  if (buffer->data != NULL) {
    buffer->allocator->free(buffer->allocator, (uint8_t*)buffer->data,
                            buffer->capacity_bytes);
  }
}

ArrowErrorCode ArrowBufferReserve(struct ArrowBuffer* buffer,
                                  int64_t min_capacity_bytes) {
  if (min_capacity_bytes > buffer->capacity_bytes) {
    int64_t new_capacity = (buffer->capacity_bytes + 1) * buffer->growth_factor;
    if (new_capacity < min_capacity_bytes) {
      new_capacity = min_capacity_bytes;
    }

    buffer->data = buffer->allocator->reallocate(buffer->allocator, buffer->data,
                                                 buffer->capacity_bytes, new_capacity);
    if (buffer->data == NULL) {
      return ENOMEM;
    }
  }

  return NANOARROW_OK;
}

ArrowErrorCode ArrowBufferReserveAdditional(struct ArrowBuffer* buffer,
                                            int64_t additional_size_bytes) {
  int result = ArrowBufferReserve(buffer, buffer->size_bytes + additional_size_bytes);
  if (result != NANOARROW_OK) {
    return result;
  }

  return NANOARROW_OK;
}

void ArrowBufferWrite(struct ArrowBuffer* buffer, struct ArrowBufferView* new_data) {
  if (new_data->n_bytes > 0) {
    memcpy(buffer->data, new_data->data, new_data->n_bytes);
    buffer->size_bytes += new_data->n_bytes;
  }
}

ArrowErrorCode ArrowBufferWriteChecked(struct ArrowBuffer* buffer,
                                       struct ArrowBufferView* new_data) {
  int result = ArrowBufferReserveAdditional(buffer, new_data->n_bytes);
  if (result != NANOARROW_OK) {
    return result;
  }

  ArrowBufferWrite(buffer, new_data);
  return NANOARROW_OK;
}
