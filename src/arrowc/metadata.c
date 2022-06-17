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
#include <stdlib.h>
#include <string.h>

#include "arrowc.h"

ArrowErrorCode ArrowMetadataWalk(const char* metadata,
                                 ArrowErrorCode (*callback)(struct ArrowStringView* key,
                                                            struct ArrowStringView* value,
                                                            void* private_data),
                                 void* private_data) {
  if (metadata == NULL) {
    return 0;
  }

  int64_t pos = 0;
  int32_t n;
  memcpy(&n, metadata + pos, sizeof(int32_t));
  pos += sizeof(int32_t);

  int result;
  struct ArrowStringView key;
  struct ArrowStringView value;

  for (int i = 0; i < n; i++) {
    int32_t name_size;
    memcpy(&name_size, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    key.data = metadata + pos;
    key.n_bytes = name_size;
    pos += name_size;

    int32_t value_size;
    memcpy(&value_size, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    value.data = metadata + pos;
    value.n_bytes = value_size;
    pos += value_size;

    result = callback(&key, &value, private_data);
    if (result != ARROWC_OK) {
      return result;
    }
  }

  return ARROWC_OK;
}

static ArrowErrorCode ArrowMetadataSizeOfCallback(struct ArrowStringView* key,
                                                  struct ArrowStringView* value,
                                                  void* private_data) {
  int64_t* size = (int64_t*)private_data;
  *size += sizeof(int32_t) + key->n_bytes + sizeof(int32_t) + value->n_bytes;
  return ARROWC_OK;
}

int64_t ArrowMetadataSizeOf(const char* metadata) {
  if (metadata == NULL) {
    return 0;
  }

  int64_t size = sizeof(int32_t);
  ArrowMetadataWalk(metadata, &ArrowMetadataSizeOfCallback, &size);
  return size;
}

struct ArrowMetadataKV {
  struct ArrowStringView* key;
  struct ArrowStringView* value;
};

#define ARROWC_EKEYFOUND 1

static ArrowErrorCode ArrowMetadataValueCallback(struct ArrowStringView* key,
                                                 struct ArrowStringView* value,
                                                 void* private_data) {
  struct ArrowMetadataKV* kv = (struct ArrowMetadataKV*)private_data;
  int key_equal = key->n_bytes == kv->key->n_bytes &&
                  strncmp(key->data, kv->key->data, key->n_bytes) == 0;
  if (key_equal) {
    kv->value->data = value->data;
    kv->value->n_bytes = value->n_bytes;
    return ARROWC_EKEYFOUND;
  } else {
    return ARROWC_OK;
  }
}

ArrowErrorCode ArrowMetadataValue(const char* metadata, const char* key,
                                  const char* default_value,
                                  struct ArrowStringView* value_out) {
  struct ArrowStringView key_view = {key, strlen(key)};
  value_out->data = default_value;
  if (default_value != NULL) {
    value_out->n_bytes = strlen(default_value);
  } else {
    value_out->n_bytes = 0;
  }
  struct ArrowMetadataKV kv = {&key_view, value_out};

  ArrowMetadataWalk(metadata, &ArrowMetadataValueCallback, &kv);
  return ARROWC_OK;
}

char ArrowMetadataContains(const char* metadata, const char* key) {
  struct ArrowStringView value;
  ArrowMetadataValue(metadata, key, NULL, &value);
  return value.data != NULL;
}
