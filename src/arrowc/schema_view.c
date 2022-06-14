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
#include <string.h>

#include "arrowc.h"

void ArrowSchemaViewSetPrimitive(struct ArrowSchemaView* schema_view,
                                 enum ArrowType data_type) {
  schema_view->data_type = data_type;
  schema_view->storage_data_type = data_type;
  schema_view->n_buffers = 2;
  schema_view->validity_buffer_id = 0;
  schema_view->data_buffer_id = 1;
}

ArrowErrorCode ArrowSchemaViewSetStorageType(struct ArrowSchemaView* schema_view,
                                             const char* format, const char** format_end,
                                             struct ArrowError* error) {
  schema_view->validity_buffer_id = -1;
  schema_view->offset_buffer_id = -1;
  schema_view->large_offset_buffer_id = -1;
  schema_view->data_buffer_id = -1;
  schema_view->type_id_buffer_id = -1;
  *format_end = format;

  switch (format[0]) {
    case 'n':
      schema_view->storage_data_type = ARROWC_TYPE_NA;
      schema_view->n_buffers = 0;
      *format_end = format + 1;
      return ARROWC_OK;
    case 'b':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_BOOL);
      *format_end = format + 1;
      return ARROWC_OK;
    case 'c':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT8);
      *format_end = format + 1;
      return ARROWC_OK;
    case 'C':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_UINT8);
      *format_end = format + 1;
      return ARROWC_OK;
    case 's':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT16);
      *format_end = format + 1;
      return ARROWC_OK;
    case 'S':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_UINT16);
      *format_end = format + 1;
      return ARROWC_OK;
    case 'i':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT32);
      *format_end = format + 1;
      return ARROWC_OK;
    case 'I':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_UINT32);
      *format_end = format + 1;
      return ARROWC_OK;
    case 'l':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT64);
      *format_end = format + 1;
      return ARROWC_OK;
    case 'L':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_UINT64);
      *format_end = format + 1;
      return ARROWC_OK;
    case 'e':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_HALF_FLOAT);
      *format_end = format + 1;
      return ARROWC_OK;
    case 'f':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_FLOAT);
      *format_end = format + 1;
      return ARROWC_OK;
    case 'g':
      ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_DOUBLE);
      *format_end = format + 1;
      return ARROWC_OK;

    // validity + data
    case 'w':
      schema_view->storage_data_type = ARROWC_TYPE_FIXED_SIZE_BINARY;
      if (format[1] != ':' || format[2] == '\0') {
        ArrowErrorSet(error, "Expected ':<width>' following 'w'");
        return EINVAL;
      }

      schema_view->n_buffers = 2;
      schema_view->validity_buffer_id = 0;
      schema_view->data_buffer_id = 1;
      schema_view->fixed_size = strtol(format + 2, (char**)format_end, 10);
      return ARROWC_OK;

    // validity + offset + data
    case 'z':
      schema_view->storage_data_type = ARROWC_TYPE_BINARY;
      schema_view->n_buffers = 3;
      schema_view->validity_buffer_id = 0;
      schema_view->offset_buffer_id = 1;
      schema_view->data_buffer_id = 2;
      *format_end = format + 1;
      return ARROWC_OK;
    case 'u':
      schema_view->storage_data_type = ARROWC_TYPE_STRING;
      schema_view->n_buffers = 3;
      schema_view->validity_buffer_id = 0;
      schema_view->offset_buffer_id = 1;
      schema_view->data_buffer_id = 2;
      *format_end = format + 1;
      return ARROWC_OK;

    // validity + large_offset + data
    case 'Z':
      schema_view->storage_data_type = ARROWC_TYPE_LARGE_BINARY;
      schema_view->n_buffers = 3;
      schema_view->validity_buffer_id = 0;
      schema_view->large_offset_buffer_id = 1;
      schema_view->data_buffer_id = 2;
      *format_end = format + 1;
      return ARROWC_OK;
    case 'U':
      schema_view->storage_data_type = ARROWC_TYPE_LARGE_STRING;
      schema_view->n_buffers = 3;
      schema_view->validity_buffer_id = 0;
      schema_view->large_offset_buffer_id = 1;
      schema_view->data_buffer_id = 2;
      *format_end = format + 1;
      return ARROWC_OK;
  }

  ArrowErrorSet(error, "Unknown format: '%s'", format);
  return EINVAL;
}

ArrowErrorCode ArrowSchemaViewInit(struct ArrowSchemaView* schema_view,
                                   struct ArrowSchema* schema, struct ArrowError* error) {
  schema_view->schema = schema;

  const char* format = schema->format;
  if (format == NULL) {
    ArrowErrorSet(
        error,
        "Error parsing schema->format: Expected a null-terminated string but found NULL");
    return EINVAL;
  }

  int format_len = strlen(format);
  if (format_len == 0) {
    ArrowErrorSet(error, "Error parsing schema->format: Expected a string with size > 0");
    return EINVAL;
  }

  const char* format_end;
  ArrowErrorCode result =
      ArrowSchemaViewSetStorageType(schema_view, format, &format_end, error);

  if (result != ARROWC_OK) {
    char child_error[1024];
    memcpy(child_error, ArrowErrorMessage(error), 1024);
    ArrowErrorSet(error, "Error parsing schema->format: %s", child_error);
    return result;
  }

  if ((format + format_len) != format_end) {
    ArrowErrorSet(error, "Error parsing schema->format '%s': parsed %d/%d characters",
                  format, (int)(format_end - format), (int)(format_len));
    return EINVAL;
  }

  // TODO: check for extension type?
  if (schema->dictionary != NULL) {
    schema_view->data_type = ARROWC_TYPE_DICTIONARY;
  }

  return ARROWC_OK;
}
