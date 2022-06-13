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

void ArrowSchemaRelease(struct ArrowSchema* schema) {
  if (schema != NULL && schema->release != NULL) {
    if (schema->format != NULL) ARROWC_FREE((void*)schema->format);
    if (schema->name != NULL) ARROWC_FREE((void*)schema->name);
    if (schema->metadata != NULL) ARROWC_FREE((void*)schema->metadata);

    // This object owns the memory for all the children, but those
    // children may have been generated elsewhere and might have
    // their own release() callback.
    if (schema->children != NULL) {
      for (int64_t i = 0; i < schema->n_children; i++) {
        if (schema->children[i] != NULL) {
          if (schema->children[i]->release != NULL) {
            schema->children[i]->release(schema->children[i]);
          }

          ARROWC_FREE(schema->children[i]);
        }
      }

      ARROWC_FREE(schema->children);
    }

    // This object owns the memory for the dictionary but it
    // may have been generated somewhere else and have its own
    // release() callback.
    if (schema->dictionary != NULL) {
      if (schema->dictionary->release != NULL) {
        schema->dictionary->release(schema->dictionary);
      }

      ARROWC_FREE(schema->dictionary);
    }

    // private data not currently used
    if (schema->private_data != NULL) {
      ARROWC_FREE(schema->private_data);
    }

    schema->release = NULL;
  }
}

int ArrowSchemaAllocate(int64_t n_children, struct ArrowSchema* schema) {
  schema->format = NULL;
  schema->name = NULL;
  schema->metadata = NULL;
  schema->flags = ARROW_FLAG_NULLABLE;
  schema->n_children = n_children;
  schema->children = NULL;
  schema->dictionary = NULL;
  schema->private_data = NULL;
  schema->release = &ArrowSchemaRelease;

  if (n_children > 0) {
    schema->children =
        (struct ArrowSchema**)ARROWC_MALLOC(n_children * sizeof(struct ArrowSchema*));

    if (schema->children == NULL) {
      schema->release(schema);
      return ENOMEM;
    }

    memset(schema->children, 0, n_children * sizeof(struct ArrowSchema*));

    for (int64_t i = 0; i < n_children; i++) {
      schema->children[i] =
          (struct ArrowSchema*)ARROWC_MALLOC(sizeof(struct ArrowSchema));

      if (schema->children[i] == NULL) {
        schema->release(schema);
        return ENOMEM;
      }

      schema->children[i]->release = NULL;
    }
  }

  // We don't allocate the dictionary because it has to be nullptr
  // for non-dictionary-encoded arrays.
  return ARROWC_OK;
}

static inline int64_t ArrowSchemaMetadataSize(const char* metadata) {
  if (metadata == NULL) {
    return 0;
  }

  int64_t pos = 0;
  int32_t n;
  memcpy(&n, metadata + pos, sizeof(int32_t));
  pos += sizeof(int32_t);

  for (int i = 0; i < n; i++) {
    int32_t name_size;
    memcpy(&name_size, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    if (name_size > 0) {
      pos += name_size;
    }

    int32_t value_size;
    memcpy(&value_size, metadata + pos, sizeof(int32_t));
    pos += sizeof(int32_t);

    if (value_size > 0) {
      pos += value_size;
    }
  }

  return pos;
}

ArrowErrorCode ArrowSchemaSetFormat(struct ArrowSchema* schema, const char* format) {
  if (format != NULL) {
    size_t format_size = strlen(format) + 1;
    schema->format = (const char*)ARROWC_MALLOC(format_size);
    if (schema->format == NULL) {
      return ENOMEM;
    }

    memcpy((void*)schema->format, format, format_size);
  } else {
    schema->format = NULL;
  }

  return ARROWC_OK;
}

ArrowErrorCode ArrowSchemaSetName(struct ArrowSchema* schema, const char* name) {
  if (name != NULL) {
    size_t name_size = strlen(name) + 1;
    schema->name = (const char*)ARROWC_MALLOC(name_size);
    if (schema->name == NULL) {
      return ENOMEM;
    }

    memcpy((void*)schema->name, name, name_size);
  } else {
    schema->name = NULL;
  }

  return ARROWC_OK;
}

ArrowErrorCode ArrowSchemaSetMetadata(struct ArrowSchema* schema, const char* metadata) {
  if (metadata != NULL) {
    size_t metadata_size = ArrowSchemaMetadataSize(metadata);
    schema->metadata = (const char*)ARROWC_MALLOC(metadata_size);
    if (schema->metadata == NULL) {
      return ENOMEM;
    }

    memcpy((void*)schema->metadata, metadata, metadata_size);
  } else {
    schema->metadata = NULL;
  }

  return ARROWC_OK;
}

int ArrowSchemaDeepCopy(struct ArrowSchema* schema, struct ArrowSchema* schema_out) {
  int result;
  result = ArrowSchemaAllocate(schema->n_children, schema_out);
  if (result != ARROWC_OK) {
    return result;
  }

  result = ArrowSchemaSetFormat(schema_out, schema->format);
  if (result != ARROWC_OK) {
    schema_out->release(schema_out);
    return result;
  }

  result = ArrowSchemaSetName(schema_out, schema->name);
  if (result != ARROWC_OK) {
    schema_out->release(schema_out);
    return result;
  }

  result = ArrowSchemaSetMetadata(schema_out, schema->metadata);
  if (result != ARROWC_OK) {
    schema_out->release(schema_out);
    return result;
  }

  for (int64_t i = 0; i < schema->n_children; i++) {
    result = ArrowSchemaDeepCopy(schema->children[i], schema_out->children[i]);
    if (result != ARROWC_OK) {
      schema_out->release(schema_out);
      return result;
    }
  }

  if (schema->dictionary != NULL) {
    schema_out->dictionary =
        (struct ArrowSchema*)ARROWC_MALLOC(sizeof(struct ArrowSchema));
    if (schema_out->dictionary == NULL) {
      schema_out->release(schema_out);
      return ENOMEM;
    }

    schema_out->dictionary->release = NULL;
    result = ArrowSchemaDeepCopy(schema->dictionary, schema_out->dictionary);
    if (result != ARROWC_OK) {
      schema_out->release(schema_out);
      return result;
    }
  }

  return ARROWC_OK;
}