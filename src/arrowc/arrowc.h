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

#ifndef ARROWC_H_INCLUDED
#define ARROWC_H_INCLUDED

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

// Extra guard for versions of Arrow without the canonical guard
#ifndef ARROW_FLAG_DICTIONARY_ORDERED

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

// EXPERIMENTAL: C stream interface

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
  // Callback to get the stream type
  // (will be the same for all arrays in the stream).
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowSchema must be released independently from the stream.
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);

  // Callback to get the next array
  // (if no error and the array is released, the stream has ended)
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowArray must be released independently from the stream.
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);

  // Callback to get optional detailed error information.
  // This must only be called if the last stream operation failed
  // with a non-0 return code.
  //
  // Return value: pointer to a null-terminated character array describing
  // the last error, or NULL if no description is available.
  //
  // The returned pointer is only valid until the next operation on this stream
  // (including release).
  const char* (*get_last_error)(struct ArrowArrayStream*);

  // Release callback: release the stream's own resources.
  // Note that arrays returned by `get_next` must be individually released.
  void (*release)(struct ArrowArrayStream*);

  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_STREAM_INTERFACE
#endif  // ARROW_FLAG_DICTIONARY_ORDERED

/// \file Arrow C Implementation
///
/// EXPERIMENTAL. Interface subject to change.

/// \page object-model Object Model
///
/// Except where noted, objects are not thread-safe and clients should
/// take care to serialize accesses to methods.
///
/// Because this library is intended to be vendored, it provides full type
/// definitions and encourages clients to stack or statically allocate
/// where convenient.

/// \defgroup arrowc-malloc Low-level memory management
/// Configure malloc(), realloc(), and free() as compile-time constants.

#ifndef ARROWC_MALLOC
/// Allocate like malloc()
#define ARROWC_MALLOC(size) malloc(size)
#endif

#ifndef ARROWC_FREE
/// Free memory allocated via malloc() or realloc()
#define ARROWC_FREE(ptr) free(ptr)
#endif

/// }@

/// \defgroup arrowc-errors Error handling primitives
/// Functions generally return an errno-compatible error code; functions that
/// need to communicate more verbose error information accept a pointer
/// to an ArrowError. This can be stack or statically allocated. The
/// content of the message is undefined unless an error code has been
/// returned.

/// \brief Error type containing a UTF-8 encoded message.
struct ArrowError {
  char message[1024];
};

/// \brief Return code for success.
#define ARROWC_OK 0

/// \brief Represents an errno-compatible error code
typedef int ArrowErrorCode;

/// }@

/// \brief Set the contents of an error using printf syntax
ArrowErrorCode ArrowErrorSet(struct ArrowError* error, const char* fmt, ...);

/// \brief Get the contents of an error
const char* ArrowErrorMessage(struct ArrowError* error);

/// \defgroup arrowc-schema Schema helpers
/// These functions allocate, copy, and destroy ArrowSchema structures

/// \brief Initialize the fields of a schema
///
/// Initializes the fields and release callback of schema_out.
ArrowErrorCode ArrowSchemaInit(int64_t n_children, struct ArrowSchema* schema_out);

/// \brief Make a (full) copy of a schema
///
/// Allocates and copies fields of schema into schema_out.
ArrowErrorCode ArrowSchemaDeepCopy(struct ArrowSchema* schema,
                                   struct ArrowSchema* schema_out);

/// \brief Copy format into schema->format
///
/// schema must have been allocated using ArrowSchemaInit or
/// ArrowSchemaDeepCopy.
ArrowErrorCode ArrowSchemaSetFormat(struct ArrowSchema* schema, const char* format);

/// \brief Copy name into schema->name
///
/// schema must have been allocated using ArrowSchemaInit or
/// ArrowSchemaDeepCopy.
ArrowErrorCode ArrowSchemaSetName(struct ArrowSchema* schema, const char* name);

/// \brief Copy metadata into schema->metadata
///
/// schema must have been allocated using ArrowSchemaInit or
/// ArrowSchemaDeepCopy.
ArrowErrorCode ArrowSchemaSetMetadata(struct ArrowSchema* schema, const char* metadata);

/// }@

#ifdef __cplusplus
}
#endif

#endif
