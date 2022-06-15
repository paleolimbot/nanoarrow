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

  // needed for decimal parsing
  const char* parse_start;
  char* parse_end;

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

    // decimal
    case 'd':
      if (format[1] != ':' || format[2] == '\0') {
        ArrowErrorSet(error, "Expected ':precision,scale[,bitwidth]' following 'd'",
                      format + 3);
        return EINVAL;
      }

      parse_start = format + 2;
      schema_view->decimal_precision = strtol(parse_start, &parse_end, 10);
      if (parse_end == parse_start || parse_end[0] != ',') {
        ArrowErrorSet(error, "Expected 'precision,scale[,bitwidth]' following 'd:'");
        return EINVAL;
      }

      parse_start = parse_end + 1;
      schema_view->decimal_scale = strtol(parse_start, &parse_end, 10);
      if (parse_end == parse_start) {
        ArrowErrorSet(error, "Expected 'scale[,bitwidth]' following 'd:precision,'");
        return EINVAL;
      } else if (parse_end[0] != ',') {
        schema_view->decimal_bitwidth = 128;
      } else {
        parse_start = parse_end + 1;
        schema_view->decimal_bitwidth = strtol(parse_start, &parse_end, 10);
        if (parse_start == parse_end) {
          ArrowErrorSet(error, "Expected precision following 'd:precision,scale,'");
          return EINVAL;
        }
      }

      *format_end = parse_end;

      switch (schema_view->decimal_bitwidth) {
        case 128:
          ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_DECIMAL128);
          return ARROWC_OK;
        case 256:
          ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_DECIMAL256);
          return ARROWC_OK;
        default:
          ArrowErrorSet(error, "Expected decimal bitwidth of 128 or 256 but found %d",
                        (int)schema_view->decimal_bitwidth);
          return EINVAL;
      }

    // validity + data
    case 'w':
      schema_view->data_type = ARROWC_TYPE_FIXED_SIZE_BINARY;
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
      schema_view->data_type = ARROWC_TYPE_BINARY;
      schema_view->storage_data_type = ARROWC_TYPE_BINARY;
      schema_view->n_buffers = 3;
      schema_view->validity_buffer_id = 0;
      schema_view->offset_buffer_id = 1;
      schema_view->data_buffer_id = 2;
      *format_end = format + 1;
      return ARROWC_OK;
    case 'u':
      schema_view->data_type = ARROWC_TYPE_STRING;
      schema_view->storage_data_type = ARROWC_TYPE_STRING;
      schema_view->n_buffers = 3;
      schema_view->validity_buffer_id = 0;
      schema_view->offset_buffer_id = 1;
      schema_view->data_buffer_id = 2;
      *format_end = format + 1;
      return ARROWC_OK;

    // validity + large_offset + data
    case 'Z':
      schema_view->data_type = ARROWC_TYPE_LARGE_BINARY;
      schema_view->storage_data_type = ARROWC_TYPE_LARGE_BINARY;
      schema_view->n_buffers = 3;
      schema_view->validity_buffer_id = 0;
      schema_view->large_offset_buffer_id = 1;
      schema_view->data_buffer_id = 2;
      *format_end = format + 1;
      return ARROWC_OK;
    case 'U':
      schema_view->data_type = ARROWC_TYPE_LARGE_STRING;
      schema_view->storage_data_type = ARROWC_TYPE_LARGE_STRING;
      schema_view->n_buffers = 3;
      schema_view->validity_buffer_id = 0;
      schema_view->large_offset_buffer_id = 1;
      schema_view->data_buffer_id = 2;
      *format_end = format + 1;
      return ARROWC_OK;

    // nested types
    case '+':
      switch (format[1]) {
        // list has validity + offset or offset
        case 'l':
          schema_view->storage_data_type = ARROWC_TYPE_LIST;
          schema_view->data_type = ARROWC_TYPE_LIST;
          schema_view->n_buffers = 2;
          schema_view->validity_buffer_id = 0;
          schema_view->offset_buffer_id = 1;
          *format_end = format + 2;
          return ARROWC_OK;

        // large list has validity + large_offset or large_offset
        case 'L':
          schema_view->storage_data_type = ARROWC_TYPE_LARGE_LIST;
          schema_view->data_type = ARROWC_TYPE_LARGE_LIST;
          schema_view->n_buffers = 2;
          schema_view->validity_buffer_id = 0;
          schema_view->large_offset_buffer_id = 1;
          *format_end = format + 2;
          return ARROWC_OK;

        // just validity buffer
        case 'w':
          if (format[2] != ':' || format[3] == '\0') {
            ArrowErrorSet(error, "Expected ':<width>' following '+w'");
            return EINVAL;
          }

          schema_view->storage_data_type = ARROWC_TYPE_FIXED_SIZE_LIST;
          schema_view->data_type = ARROWC_TYPE_FIXED_SIZE_LIST;
          schema_view->n_buffers = 1;
          schema_view->validity_buffer_id = 0;
          schema_view->fixed_size = strtol(format + 3, (char**)format_end, 10);
          return ARROWC_OK;
        case 's':
          schema_view->storage_data_type = ARROWC_TYPE_STRUCT;
          schema_view->data_type = ARROWC_TYPE_STRUCT;
          schema_view->n_buffers = 1;
          schema_view->validity_buffer_id = 0;
          *format_end = format + 2;
          return ARROWC_OK;
        case 'm':
          schema_view->storage_data_type = ARROWC_TYPE_MAP;
          schema_view->data_type = ARROWC_TYPE_MAP;
          schema_view->n_buffers = 1;
          schema_view->validity_buffer_id = 0;
          *format_end = format + 2;
          return ARROWC_OK;

        // unions
        case 'u':
          switch (format[2]) {
            case 'd':
              schema_view->storage_data_type = ARROWC_TYPE_DENSE_UNION;
              schema_view->data_type = ARROWC_TYPE_DENSE_UNION;
              schema_view->n_buffers = 2;
              schema_view->type_id_buffer_id = 0;
              schema_view->offset_buffer_id = 1;
              break;
            case 's':
              schema_view->storage_data_type = ARROWC_TYPE_SPARSE_UNION;
              schema_view->data_type = ARROWC_TYPE_SPARSE_UNION;
              schema_view->n_buffers = 1;
              schema_view->type_id_buffer_id = 0;
              break;
            default:
              ArrowErrorSet(error,
                            "Expected union format string +us:<type_ids> or "
                            "+ud:<type_ids> but found '%s'",
                            format);
              return EINVAL;
          }

          if (format[3] == ':') {
            schema_view->union_type_ids.data = format + 4;
            schema_view->union_type_ids.n_bytes = strlen(format + 4);
            *format_end = format + strlen(format);
            return ARROWC_OK;
          } else {
            ArrowErrorSet(error,
                          "Expected union format string +us:<type_ids> or +ud:<type_ids> "
                          "but found '%s'",
                          format);
            return EINVAL;
          }
      }

    // date/time types
    case 't':
      switch (format[1]) {
        // date
        case 'd':
          switch (format[2]) {
            case 'D':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT32);
              schema_view->data_type = ARROWC_TYPE_DATE32;
              *format_end = format + 3;
              return ARROWC_OK;
            case 'm':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT64);
              schema_view->data_type = ARROWC_TYPE_DATE64;
              *format_end = format + 3;
              return ARROWC_OK;
            default:
              ArrowErrorSet(error, "Expected 'D' or 'm' following 'td' but found '%s'",
                            format + 2);
              return EINVAL;
          }

        // time of day
        case 't':
          switch (format[2]) {
            case 's':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT32);
              schema_view->data_type = ARROWC_TYPE_TIME32;
              schema_view->time_unit = ARROWC_TIME_UNIT_SECOND;
              *format_end = format + 3;
              return ARROWC_OK;
            case 'm':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT32);
              schema_view->data_type = ARROWC_TYPE_TIME32;
              schema_view->time_unit = ARROWC_TIME_UNIT_MILLI;
              *format_end = format + 3;
              return ARROWC_OK;
            case 'u':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT64);
              schema_view->data_type = ARROWC_TYPE_TIME64;
              schema_view->time_unit = ARROWC_TIME_UNIT_MICRO;
              *format_end = format + 3;
              return ARROWC_OK;
            case 'n':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT64);
              schema_view->data_type = ARROWC_TYPE_TIME64;
              schema_view->time_unit = ARROWC_TIME_UNIT_NANO;
              *format_end = format + 3;
              return ARROWC_OK;
            default:
              ArrowErrorSet(
                  error, "Expected 's', 'm', 'u', or 'n' following 'tt' bur found '%s'",
                  format + 2);
              return EINVAL;
          }

        // timestamp
        case 's':
          switch (format[2]) {
            case 's':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT32);
              schema_view->data_type = ARROWC_TYPE_TIMESTAMP;
              schema_view->time_unit = ARROWC_TIME_UNIT_SECOND;
              break;
            case 'm':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT32);
              schema_view->data_type = ARROWC_TYPE_TIMESTAMP;
              schema_view->time_unit = ARROWC_TIME_UNIT_MILLI;
              break;
            case 'u':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT64);
              schema_view->data_type = ARROWC_TYPE_TIMESTAMP;
              schema_view->time_unit = ARROWC_TIME_UNIT_MICRO;
              break;
            case 'n':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT64);
              schema_view->data_type = ARROWC_TYPE_TIMESTAMP;
              schema_view->time_unit = ARROWC_TIME_UNIT_NANO;
              break;
            default:
              ArrowErrorSet(
                  error, "Expected 's', 'm', 'u', or 'n' following 'ts' but found '%s'",
                  format + 2);
              return EINVAL;
          }

          if (format[3] != ':') {
            ArrowErrorSet(error, "Expected ':' following '%.3s' but found '%s'", format,
                          format + 3);
            return EINVAL;
          }

          schema_view->timezone.data = format + 4;
          schema_view->timezone.n_bytes = strlen(format + 4);
          *format_end = format + strlen(format);
          return ARROWC_OK;

        // duration
        case 'D':
          switch (format[2]) {
            case 's':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT32);
              schema_view->data_type = ARROWC_TYPE_DURATION;
              schema_view->time_unit = ARROWC_TIME_UNIT_SECOND;
              *format_end = format + 3;
              return ARROWC_OK;
            case 'm':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT32);
              schema_view->data_type = ARROWC_TYPE_DURATION;
              schema_view->time_unit = ARROWC_TIME_UNIT_MILLI;
              *format_end = format + 3;
              return ARROWC_OK;
            case 'u':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT64);
              schema_view->data_type = ARROWC_TYPE_DURATION;
              schema_view->time_unit = ARROWC_TIME_UNIT_MICRO;
              *format_end = format + 3;
              return ARROWC_OK;
            case 'n':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INT64);
              schema_view->data_type = ARROWC_TYPE_DURATION;
              schema_view->time_unit = ARROWC_TIME_UNIT_NANO;
              *format_end = format + 3;
              return ARROWC_OK;
            default:
              ArrowErrorSet(error,
                            "Expected 's', 'm', u', or 'n' following 'tD' but found '%s'",
                            format + 2);
              return EINVAL;
          }

        // interval
        case 'i':
          switch (format[2]) {
            case 'M':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INTERVAL_MONTHS);
              *format_end = format + 3;
              return ARROWC_OK;
            case 'D':
              ArrowSchemaViewSetPrimitive(schema_view, ARROWC_TYPE_INTERVAL_DAY_TIME);
              *format_end = format + 3;
              return ARROWC_OK;
            case 'n':
              ArrowSchemaViewSetPrimitive(schema_view,
                                          ARROWC_TYPE_INTERVAL_MONTH_DAY_NANO);
              *format_end = format + 3;
              return ARROWC_OK;
            default:
              ArrowErrorSet(error, "Expected 'M' or 'D' following 'ti' but found '%s'",
                            format + 2);
              return EINVAL;
          }

        default:
          ArrowErrorSet(
              error, "Expected 'd', 't', 's', 'D', or 'i' following 't' but found '%s'",
              format + 1);
          return EINVAL;
      }

    default:
      ArrowErrorSet(error, "Unknown format: '%s'", format);
      return EINVAL;
  }
}

ArrowErrorCode ArrowSchemaViewInit(struct ArrowSchemaView* schema_view,
                                   struct ArrowSchema* schema, struct ArrowError* error) {
  if (schema == NULL) {
    ArrowErrorSet(error, "Expected non-NULL schema");
    return EINVAL;
  }

  if (schema->release == NULL) {
    ArrowErrorSet(error, "Expected non-released schema");
    return EINVAL;
  }

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
    // TODO: check for valid index type?
  }

  return ARROWC_OK;
}
