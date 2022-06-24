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

#include "nanoarrow.h"

void ArrowSchemaRelease(struct ArrowSchema* schema) {
  if (schema->format != NULL) ArrowFree((void*)schema->format);
  if (schema->name != NULL) ArrowFree((void*)schema->name);
  if (schema->metadata != NULL) ArrowFree((void*)schema->metadata);

  // This object owns the memory for all the children, but those
  // children may have been generated elsewhere and might have
  // their own release() callback.
  if (schema->children != NULL) {
    for (int64_t i = 0; i < schema->n_children; i++) {
      if (schema->children[i] != NULL) {
        if (schema->children[i]->release != NULL) {
          schema->children[i]->release(schema->children[i]);
        }

        ArrowFree(schema->children[i]);
      }
    }

    ArrowFree(schema->children);
  }

  // This object owns the memory for the dictionary but it
  // may have been generated somewhere else and have its own
  // release() callback.
  if (schema->dictionary != NULL) {
    if (schema->dictionary->release != NULL) {
      schema->dictionary->release(schema->dictionary);
    }

    ArrowFree(schema->dictionary);
  }

  // private data not currently used
  if (schema->private_data != NULL) {
    ArrowFree(schema->private_data);
  }

  schema->release = NULL;
}

int ArrowSchemaInit(struct ArrowSchema* schema) {
  schema->format = NULL;
  schema->name = NULL;
  schema->metadata = NULL;
  schema->flags = ARROW_FLAG_NULLABLE;
  schema->n_children = 0;
  schema->children = NULL;
  schema->dictionary = NULL;
  schema->private_data = NULL;
  schema->release = &ArrowSchemaRelease;

  // We don't allocate the dictionary because it has to be nullptr
  // for non-dictionary-encoded arrays.
  return NANOARROW_OK;
}

ArrowErrorCode ArrowSchemaSetFormat(struct ArrowSchema* schema, const char* format) {
  if (schema->format != NULL) {
    ArrowFree((void*)schema->format);
  }

  if (format != NULL) {
    size_t format_size = strlen(format) + 1;
    schema->format = (const char*)ArrowMalloc(format_size);
    if (schema->format == NULL) {
      return ENOMEM;
    }

    memcpy((void*)schema->format, format, format_size);
  } else {
    schema->format = NULL;
  }

  return NANOARROW_OK;
}

ArrowErrorCode ArrowSchemaSetName(struct ArrowSchema* schema, const char* name) {
  if (schema->name != NULL) {
    ArrowFree((void*)schema->name);
  }

  if (name != NULL) {
    size_t name_size = strlen(name) + 1;
    schema->name = (const char*)ArrowMalloc(name_size);
    if (schema->name == NULL) {
      return ENOMEM;
    }

    memcpy((void*)schema->name, name, name_size);
  } else {
    schema->name = NULL;
  }

  return NANOARROW_OK;
}

ArrowErrorCode ArrowSchemaSetMetadata(struct ArrowSchema* schema, const char* metadata) {
  if (schema->metadata != NULL) {
    ArrowFree((void*)schema->metadata);
  }

  if (metadata != NULL) {
    size_t metadata_size = ArrowMetadataSizeOf(metadata);
    schema->metadata = (const char*)ArrowMalloc(metadata_size);
    if (schema->metadata == NULL) {
      return ENOMEM;
    }

    memcpy((void*)schema->metadata, metadata, metadata_size);
  } else {
    schema->metadata = NULL;
  }

  return NANOARROW_OK;
}

ArrowErrorCode ArrowSchemaAllocateChildren(struct ArrowSchema* schema,
                                           int64_t n_children) {
  if (schema->children != NULL) {
    return EEXIST;
  }

  if (n_children > 0) {
    schema->children =
        (struct ArrowSchema**)ArrowMalloc(n_children * sizeof(struct ArrowSchema*));

    if (schema->children == NULL) {
      return ENOMEM;
    }

    schema->n_children = n_children;

    memset(schema->children, 0, n_children * sizeof(struct ArrowSchema*));

    for (int64_t i = 0; i < n_children; i++) {
      schema->children[i] = (struct ArrowSchema*)ArrowMalloc(sizeof(struct ArrowSchema));

      if (schema->children[i] == NULL) {
        return ENOMEM;
      }

      schema->children[i]->release = NULL;
    }
  }

  return NANOARROW_OK;
}

ArrowErrorCode ArrowSchemaAllocateDictionary(struct ArrowSchema* schema) {
  if (schema->dictionary != NULL) {
    return EEXIST;
  }

  schema->dictionary = (struct ArrowSchema*)ArrowMalloc(sizeof(struct ArrowSchema));
  if (schema->dictionary == NULL) {
    return ENOMEM;
  }

  schema->dictionary->release = NULL;
  return NANOARROW_OK;
}

int ArrowSchemaDeepCopy(struct ArrowSchema* schema, struct ArrowSchema* schema_out) {
  int result;
  result = ArrowSchemaInit(schema_out);
  if (result != NANOARROW_OK) {
    return result;
  }

  result = ArrowSchemaSetFormat(schema_out, schema->format);
  if (result != NANOARROW_OK) {
    schema_out->release(schema_out);
    return result;
  }

  result = ArrowSchemaSetName(schema_out, schema->name);
  if (result != NANOARROW_OK) {
    schema_out->release(schema_out);
    return result;
  }

  result = ArrowSchemaSetMetadata(schema_out, schema->metadata);
  if (result != NANOARROW_OK) {
    schema_out->release(schema_out);
    return result;
  }

  result = ArrowSchemaAllocateChildren(schema_out, schema->n_children);
  if (result != NANOARROW_OK) {
    schema_out->release(schema_out);
    return result;
  }

  for (int64_t i = 0; i < schema->n_children; i++) {
    result = ArrowSchemaDeepCopy(schema->children[i], schema_out->children[i]);
    if (result != NANOARROW_OK) {
      schema_out->release(schema_out);
      return result;
    }
  }

  if (schema->dictionary != NULL) {
    result = ArrowSchemaAllocateDictionary(schema_out);
    if (result != NANOARROW_OK) {
      schema_out->release(schema_out);
      return result;
    }

    result = ArrowSchemaDeepCopy(schema->dictionary, schema_out->dictionary);
    if (result != NANOARROW_OK) {
      schema_out->release(schema_out);
      return result;
    }
  }

  return NANOARROW_OK;
}
