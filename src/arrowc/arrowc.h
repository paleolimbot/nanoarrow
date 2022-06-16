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

/// \brief Set the contents of an error using printf syntax
ArrowErrorCode ArrowErrorSet(struct ArrowError* error, const char* fmt, ...);

/// \brief Get the contents of an error
const char* ArrowErrorMessage(struct ArrowError* error);

/// }@

/// \defgroup arrowc-utils Utility data structures

/// \brief An non-owning view of a string
struct ArrowStringView {
  /// \brief A pointer to the start of the string
  ///
  /// If n_bytes is 0, this value may be NULL.
  const char* data;

  /// \brief The size of the string in bytes,
  ///
  /// (Not including the null terminator.)
  int64_t n_bytes;
};

/// \brief Arrow type enumerator
enum ArrowType {
  ARROWC_TYPE_NA,
  ARROWC_TYPE_BOOL,
  ARROWC_TYPE_UINT8,
  ARROWC_TYPE_INT8,
  ARROWC_TYPE_UINT16,
  ARROWC_TYPE_INT16,
  ARROWC_TYPE_UINT32,
  ARROWC_TYPE_INT32,
  ARROWC_TYPE_UINT64,
  ARROWC_TYPE_INT64,
  ARROWC_TYPE_HALF_FLOAT,
  ARROWC_TYPE_FLOAT,
  ARROWC_TYPE_DOUBLE,
  ARROWC_TYPE_STRING,
  ARROWC_TYPE_BINARY,
  ARROWC_TYPE_FIXED_SIZE_BINARY,
  ARROWC_TYPE_DATE32,
  ARROWC_TYPE_DATE64,
  ARROWC_TYPE_TIMESTAMP,
  ARROWC_TYPE_TIME32,
  ARROWC_TYPE_TIME64,
  ARROWC_TYPE_INTERVAL_MONTHS,
  ARROWC_TYPE_INTERVAL_DAY_TIME,
  ARROWC_TYPE_DECIMAL128,
  ARROWC_TYPE_DECIMAL256,
  ARROWC_TYPE_LIST,
  ARROWC_TYPE_STRUCT,
  ARROWC_TYPE_SPARSE_UNION,
  ARROWC_TYPE_DENSE_UNION,
  ARROWC_TYPE_DICTIONARY,
  ARROWC_TYPE_MAP,
  ARROWC_TYPE_EXTENSION,
  ARROWC_TYPE_FIXED_SIZE_LIST,
  ARROWC_TYPE_DURATION,
  ARROWC_TYPE_LARGE_STRING,
  ARROWC_TYPE_LARGE_BINARY,
  ARROWC_TYPE_LARGE_LIST,
  ARROWC_TYPE_INTERVAL_MONTH_DAY_NANO
};

/// \brief Arrow time unit enumerator
enum ArrowTimeUnit {
  ARROWC_TIME_UNIT_SECOND = 0,
  ARROWC_TIME_UNIT_MILLI = 1,
  ARROWC_TIME_UNIT_MICRO = 2,
  ARROWC_TIME_UNIT_NANO = 3
};

/// }@

/// \defgroup arrowc-schema Schema producer helpers
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

/// \defgroup arrowc-schema-view Schema consumer helpers

/// \brief A non-owning view of a parsed ArrowSchema
///
/// Contains more readily extractable values than a raw ArrowSchema.
/// Clients can stack or statically allocate this structure but are
/// encouraged to use the provided getters to ensure forward
/// compatiblity.
struct ArrowSchemaView {
  /// A pointer to the schema represented by this view
  struct ArrowSchema* schema;

  /// \brief The data type represented by the schema
  ///
  /// This value may be ARROWC_TYPE_DICTIONARY if the schema has a
  /// non-null dictionary member; this value may be ARROWC_TYPE_EXTENSION
  /// if the schema has the ARROW:extension:name metadata field set.
  /// Datetime types are valid values.
  enum ArrowType data_type;

  /// \brief The storage data type represented by the schema
  ///
  /// This value will never be ARROWC_TYPE_DICTIONARY, ARROWC_TYPE_EXTENSION
  /// or any datetime type. This value represents only the type required to
  /// interpret the buffers in the array.
  enum ArrowType storage_data_type;

  /// \brief The expected number of buffers in a paired ArrowArray
  int32_t n_buffers;

  /// \brief The index of the validity buffer or -1 if one does not exist
  int32_t validity_buffer_id;

  /// \brief The index of the offset buffer or -1 if one does not exist
  int32_t offset_buffer_id;

  /// \brief The index of the large_offset buffer or -1 if one does not exist
  int32_t large_offset_buffer_id;

  /// \brief The index of the data buffer or -1 if one does not exist
  int32_t data_buffer_id;

  /// \brief The index of the type_ids buffer or -1 if one does not exist
  int32_t type_id_buffer_id;

  /// \brief Format fixed size parameter
  ///
  /// This value is set when parsing a fixed-size binary or fixed-size
  /// list schema; this value is undefined for other types.
  int32_t fixed_size;

  /// \brief Decimal bitwidth
  ///
  /// This value is set when parsing a decimal type schema;
  /// this value is undefined for other types.
  int32_t decimal_bitwidth;

  /// \brief Decimal precision
  ///
  /// This value is set when parsing a decimal type schema;
  /// this value is undefined for other types.
  int32_t decimal_precision;

  /// \brief Decimal scale
  ///
  /// This value is set when parsing a decimal type schema;
  /// this value is undefined for other types.
  int32_t decimal_scale;

  /// \brief Format time unit parameter
  ///
  /// This value is set when parsing a date/time type. The value is
  /// undefined for other types.
  enum ArrowTimeUnit time_unit;

  /// \brief Format timezone parameter
  ///
  /// This value is set when parsing a timestamp type and represents
  /// the timezone format parameter. The ArrowStrintgView points to
  /// data within the schema and the value is undefined for other types.
  struct ArrowStringView timezone;

  /// \brief Union type ids parameter
  ///
  /// This value is set when parsing a union type and represents
  /// type ids parameter. The ArrowStringView points to
  /// data within the schema and the value is undefined for other types.
  struct ArrowStringView union_type_ids;
};

/// \brief Initialize an ArrowSchemaView
ArrowErrorCode ArrowSchemaViewInit(struct ArrowSchemaView* schema_view,
                                   struct ArrowSchema* schema, struct ArrowError* error);

/// }@

#ifdef __cplusplus
}
#endif

#endif
