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

#include <gtest/gtest.h>

#include <arrow/c/bridge.h>
#include <arrow/testing/gtest_util.h>

#include "arrowc/arrowc.h"

using namespace arrow;

TEST(SchemaViewTest, SchemaViewInitErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, nullptr, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Expected non-NULL schema");

  schema.release = nullptr;
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Expected non-released schema");

  ASSERT_EQ(ArrowSchemaInit(0, &schema), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(
      ArrowErrorMessage(&error),
      "Error parsing schema->format: Expected a null-terminated string but found NULL");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, ""), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected a string with size > 0");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "w"), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':<width>' following 'w'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "w:"), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':<width>' following 'w'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "w:abc"), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format 'w:abc': parsed 2/5 characters");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "*"), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Unknown format: '*'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "n*"), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format 'n*': parsed 1/2 characters");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+w"), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':<width>' following '+w'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+w:"), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':<width>' following '+w'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+u*"), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected union format string "
               "+us:<type_ids> or +ud:<type_ids> but found '+u*'");

  // missing colon
  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+us"), ARROWC_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected union format string "
               "+us:<type_ids> or +ud:<type_ids> but found '+us'");

  schema.release(&schema);
}

#define EXPECT_SIMPLE_TYPE_PARSE_WORKED(arrow_t, arrowc_t)                  \
  ARROW_EXPECT_OK(ExportType(*arrow_t, &schema));                           \
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK); \
  EXPECT_EQ(schema_view.n_buffers, 2);                                      \
  EXPECT_EQ(schema_view.validity_buffer_id, 0);                             \
  EXPECT_EQ(schema_view.data_buffer_id, 1);                                 \
  EXPECT_EQ(schema_view.data_type, arrowc_t);                               \
  EXPECT_EQ(schema_view.storage_data_type, arrowc_t);                       \
  schema.release(&schema)

TEST(SchemaViewTest, SchemaViewInitSimple) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*null(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 0);
  schema.release(&schema);

  EXPECT_SIMPLE_TYPE_PARSE_WORKED(boolean(), ARROWC_TYPE_BOOL);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(int8(), ARROWC_TYPE_INT8);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(uint8(), ARROWC_TYPE_UINT8);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(int16(), ARROWC_TYPE_INT16);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(uint16(), ARROWC_TYPE_UINT16);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(int32(), ARROWC_TYPE_INT32);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(uint32(), ARROWC_TYPE_UINT32);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(int64(), ARROWC_TYPE_INT64);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(uint64(), ARROWC_TYPE_UINT64);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(float16(), ARROWC_TYPE_HALF_FLOAT);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(float64(), ARROWC_TYPE_DOUBLE);
  EXPECT_SIMPLE_TYPE_PARSE_WORKED(float32(), ARROWC_TYPE_FLOAT);
}

TEST(SchemaViewTest, SchemaViewInitDecimal) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*decimal128(5, 6), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_DECIMAL128);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_DECIMAL128);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*decimal256(5, 6), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_DECIMAL256);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_DECIMAL256);
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitBinaryAndString) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*fixed_size_binary(123), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_FIXED_SIZE_BINARY);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_FIXED_SIZE_BINARY);
  EXPECT_EQ(schema_view.fixed_size, 123);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*utf8(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 3);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.offset_buffer_id, 1);
  EXPECT_EQ(schema_view.data_buffer_id, 2);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_STRING);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_STRING);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*binary(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 3);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.offset_buffer_id, 1);
  EXPECT_EQ(schema_view.data_buffer_id, 2);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_BINARY);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_BINARY);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*large_binary(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 3);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.large_offset_buffer_id, 1);
  EXPECT_EQ(schema_view.data_buffer_id, 2);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_LARGE_BINARY);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_LARGE_BINARY);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*large_utf8(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 3);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.large_offset_buffer_id, 1);
  EXPECT_EQ(schema_view.data_buffer_id, 2);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_LARGE_STRING);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_LARGE_STRING);
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeDate) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*date32(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_DATE32);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT32);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*date64(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_DATE64);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT64);
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeTime) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*time32(TimeUnit::SECOND), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_TIME32);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT32);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_SECOND);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*time32(TimeUnit::MILLI), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_TIME32);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT32);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_MILLI);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*time64(TimeUnit::MICRO), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_TIME64);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_MICRO);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*time64(TimeUnit::NANO), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_TIME64);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_NANO);
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeTimestamp) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*timestamp(TimeUnit::SECOND, "America/Halifax"), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_TIMESTAMP);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT32);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_SECOND);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*timestamp(TimeUnit::MILLI, "America/Halifax"), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_TIMESTAMP);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT32);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_MILLI);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*timestamp(TimeUnit::MICRO, "America/Halifax"), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_TIMESTAMP);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_MICRO);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*timestamp(TimeUnit::NANO, "America/Halifax"), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_TIMESTAMP);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_NANO);
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeDuration) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*duration(TimeUnit::SECOND), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_DURATION);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT32);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_SECOND);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*duration(TimeUnit::MILLI), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_DURATION);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT32);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_MILLI);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*duration(TimeUnit::MICRO), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_DURATION);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_MICRO);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*duration(TimeUnit::NANO), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_DURATION);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, ARROWC_TIME_UNIT_NANO);
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeInterval) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*month_interval(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_INTERVAL_MONTHS);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INTERVAL_MONTHS);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*day_time_interval(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_INTERVAL_DAY_TIME);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INTERVAL_DAY_TIME);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*month_day_nano_interval(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_INTERVAL_MONTH_DAY_NANO);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_INTERVAL_MONTH_DAY_NANO);
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitNestedList) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*list(int32()), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.offset_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_LIST);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_LIST);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*large_list(int32()), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.large_offset_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_LARGE_LIST);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_LARGE_LIST);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*fixed_size_list(int32(), 123), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 1);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_FIXED_SIZE_LIST);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_FIXED_SIZE_LIST);
  EXPECT_EQ(schema_view.fixed_size, 123);
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitNestedMapAndStruct) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*struct_({field("col", int32())}), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 1);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_STRUCT);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_STRUCT);
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*map(int32(), int32()), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 1);
  EXPECT_EQ(schema_view.validity_buffer_id, 0);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_MAP);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_MAP);
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitNestedUnion) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*dense_union({field("col", int32())}), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 2);
  EXPECT_EQ(schema_view.type_id_buffer_id, 0);
  EXPECT_EQ(schema_view.offset_buffer_id, 1);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_DENSE_UNION);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_DENSE_UNION);
  EXPECT_EQ(
      std::string(schema_view.union_type_ids.data, schema_view.union_type_ids.n_bytes),
      std::string("0"));
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*sparse_union({field("col", int32())}), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), ARROWC_OK);
  EXPECT_EQ(schema_view.n_buffers, 1);
  EXPECT_EQ(schema_view.type_id_buffer_id, 0);
  EXPECT_EQ(schema_view.data_type, ARROWC_TYPE_SPARSE_UNION);
  EXPECT_EQ(schema_view.storage_data_type, ARROWC_TYPE_SPARSE_UNION);
  EXPECT_EQ(
      std::string(schema_view.union_type_ids.data, schema_view.union_type_ids.n_bytes),
      std::string("0"));
  schema.release(&schema);
}
