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
#include <arrow/util/key_value_metadata.h>

#include "nanoarrow/nanoarrow.h"

using namespace arrow;

TEST(SchemaTest, SchemaInit) {
  struct ArrowSchema schema;
  ASSERT_EQ(ArrowSchemaInit(&schema, NANOARROW_TYPE_INVALID), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&schema, 2), NANOARROW_OK);

  ASSERT_NE(schema.release, nullptr);
  EXPECT_EQ(schema.format, nullptr);
  EXPECT_EQ(schema.name, nullptr);
  EXPECT_EQ(schema.metadata, nullptr);
  EXPECT_EQ(schema.n_children, 2);
  EXPECT_EQ(schema.children[0]->release, nullptr);
  EXPECT_EQ(schema.children[1]->release, nullptr);

  schema.release(&schema);
  EXPECT_EQ(schema.release, nullptr);
}

TEST(SchemaTest, SchemaInitFixedSize) {
  struct ArrowSchema schema;

  EXPECT_EQ(ArrowSchemaInitFixedSize(&schema, NANOARROW_TYPE_DOUBLE, 1), EINVAL);
  EXPECT_EQ(schema.release, nullptr);
  EXPECT_EQ(ArrowSchemaInitFixedSize(&schema, NANOARROW_TYPE_FIXED_SIZE_BINARY, 0),
            EINVAL);
  EXPECT_EQ(schema.release, nullptr);

  EXPECT_EQ(ArrowSchemaInitFixedSize(&schema, NANOARROW_TYPE_FIXED_SIZE_BINARY, 45),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "w:45");
  schema.release(&schema);

  EXPECT_EQ(ArrowSchemaInitFixedSize(&schema, NANOARROW_TYPE_FIXED_SIZE_LIST, 12),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+w:12");
  schema.release(&schema);
}

TEST(SchemaTest, SchemaInitDecimal) {
  struct ArrowSchema schema;

  EXPECT_EQ(ArrowSchemaInitDecimal(&schema, NANOARROW_TYPE_DECIMAL128, -1, 1), EINVAL);
  EXPECT_EQ(schema.release, nullptr);
  EXPECT_EQ(ArrowSchemaInitDecimal(&schema, NANOARROW_TYPE_DOUBLE, 1, 2), EINVAL);
  EXPECT_EQ(schema.release, nullptr);

  EXPECT_EQ(ArrowSchemaInitDecimal(&schema, NANOARROW_TYPE_DECIMAL128, 1, 2),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "d:1,2");
  schema.release(&schema);

  EXPECT_EQ(ArrowSchemaInitDecimal(&schema, NANOARROW_TYPE_DECIMAL256, 1, 2),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "d:1,2,256");
  schema.release(&schema);
}

TEST(SchemaTest, SchemaInitDateTime) {
  struct ArrowSchema schema;

  EXPECT_EQ(ArrowSchemaInitDateTime(&schema, NANOARROW_TYPE_DOUBLE,
                                    NANOARROW_TIME_UNIT_SECOND, nullptr),
            EINVAL);
  EXPECT_EQ(schema.release, nullptr);
  EXPECT_EQ(ArrowSchemaInitDateTime(&schema, NANOARROW_TYPE_TIME32,
                                    NANOARROW_TIME_UNIT_SECOND, "non-null timezone"),
            EINVAL);
  EXPECT_EQ(schema.release, nullptr);
  EXPECT_EQ(ArrowSchemaInitDateTime(&schema, NANOARROW_TYPE_DURATION,
                                    NANOARROW_TIME_UNIT_SECOND, "non-null timezone"),
            EINVAL);
  EXPECT_EQ(schema.release, nullptr);

  EXPECT_EQ(ArrowSchemaInitDateTime(&schema, NANOARROW_TYPE_TIME32,
                                    NANOARROW_TIME_UNIT_SECOND, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tts");
  schema.release(&schema);

  EXPECT_EQ(ArrowSchemaInitDateTime(&schema, NANOARROW_TYPE_TIME64,
                                    NANOARROW_TIME_UNIT_NANO, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "ttn");
  schema.release(&schema);

  EXPECT_EQ(ArrowSchemaInitDateTime(&schema, NANOARROW_TYPE_DURATION,
                                    NANOARROW_TIME_UNIT_SECOND, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tDs");
  schema.release(&schema);

  EXPECT_EQ(ArrowSchemaInitDateTime(&schema, NANOARROW_TYPE_TIMESTAMP,
                                    NANOARROW_TIME_UNIT_SECOND, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tss:");
  schema.release(&schema);

  EXPECT_EQ(ArrowSchemaInitDateTime(&schema, NANOARROW_TYPE_TIMESTAMP,
                                    NANOARROW_TIME_UNIT_SECOND, "America/Halifax"),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tss:America/Halifax");
  schema.release(&schema);
}

TEST(SchemaTest, SchemaSetFormat) {
  struct ArrowSchema schema;
  ArrowSchemaInit(&schema, NANOARROW_TYPE_INVALID);

  EXPECT_EQ(ArrowSchemaSetFormat(&schema, "i"), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "i");

  EXPECT_EQ(ArrowSchemaSetFormat(&schema, nullptr), NANOARROW_OK);
  EXPECT_EQ(schema.format, nullptr);

  schema.release(&schema);
}

TEST(SchemaTest, SchemaSetName) {
  struct ArrowSchema schema;
  ArrowSchemaInit(&schema, NANOARROW_TYPE_INVALID);

  EXPECT_EQ(ArrowSchemaSetName(&schema, "a_name"), NANOARROW_OK);
  EXPECT_STREQ(schema.name, "a_name");

  EXPECT_EQ(ArrowSchemaSetName(&schema, nullptr), NANOARROW_OK);
  EXPECT_EQ(schema.name, nullptr);

  schema.release(&schema);
}

TEST(SchemaTest, SchemaSetMetadata) {
  struct ArrowSchema schema;
  ArrowSchemaInit(&schema, NANOARROW_TYPE_INVALID);

  // (test will only work on little endian)
  char simple_metadata[] = {'\1', '\0', '\0', '\0', '\3', '\0', '\0', '\0', 'k', 'e',
                            'y',  '\5', '\0', '\0', '\0', 'v',  'a',  'l',  'u', 'e'};

  EXPECT_EQ(ArrowSchemaSetMetadata(&schema, simple_metadata), NANOARROW_OK);
  EXPECT_EQ(memcmp(schema.metadata, simple_metadata, sizeof(simple_metadata)), 0);

  EXPECT_EQ(ArrowSchemaSetMetadata(&schema, nullptr), NANOARROW_OK);
  EXPECT_EQ(schema.metadata, nullptr);

  schema.release(&schema);
}

TEST(SchemaTest, SchemaAllocateDictionary) {
  struct ArrowSchema schema;
  ArrowSchemaInit(&schema, NANOARROW_TYPE_INVALID);

  EXPECT_EQ(ArrowSchemaAllocateDictionary(&schema), NANOARROW_OK);
  EXPECT_EQ(schema.dictionary->release, nullptr);
  EXPECT_EQ(ArrowSchemaAllocateDictionary(&schema), EEXIST);
  schema.release(&schema);
}

TEST(SchemaTest, SchemaCopySimpleType) {
  struct ArrowSchema schema;
  ARROW_EXPECT_OK(ExportType(*int32(), &schema));

  struct ArrowSchema schema_copy;
  ArrowSchemaDeepCopy(&schema, &schema_copy);

  ASSERT_NE(schema_copy.release, nullptr);
  EXPECT_STREQ(schema.format, "i");

  schema.release(&schema);
  schema_copy.release(&schema_copy);
}

TEST(SchemaTest, SchemaCopyNestedType) {
  struct ArrowSchema schema;
  auto struct_type = struct_({field("col1", int32())});
  ARROW_EXPECT_OK(ExportType(*struct_type, &schema));

  struct ArrowSchema schema_copy;
  ArrowSchemaDeepCopy(&schema, &schema_copy);

  ASSERT_NE(schema_copy.release, nullptr);
  EXPECT_STREQ(schema_copy.format, "+s");
  EXPECT_EQ(schema_copy.n_children, 1);
  EXPECT_STREQ(schema_copy.children[0]->format, "i");
  EXPECT_STREQ(schema_copy.children[0]->name, "col1");

  schema.release(&schema);
  schema_copy.release(&schema_copy);
}

TEST(SchemaTest, SchemaCopyDictType) {
  struct ArrowSchema schema;
  auto struct_type = dictionary(int32(), int64());
  ARROW_EXPECT_OK(ExportType(*struct_type, &schema));

  struct ArrowSchema schema_copy;
  ArrowSchemaDeepCopy(&schema, &schema_copy);

  ASSERT_STREQ(schema_copy.format, "i");
  ASSERT_NE(schema_copy.dictionary, nullptr);
  EXPECT_STREQ(schema_copy.dictionary->format, "l");

  schema.release(&schema);
  schema_copy.release(&schema_copy);
}

TEST(SchemaTest, SchemaCopyMetadata) {
  struct ArrowSchema schema;
  auto arrow_meta = std::make_shared<KeyValueMetadata>();
  arrow_meta->Append("some_key", "some_value");

  auto int_field = field("field_name", int32(), arrow_meta);
  ARROW_EXPECT_OK(ExportField(*int_field, &schema));

  struct ArrowSchema schema_copy;
  ArrowSchemaDeepCopy(&schema, &schema_copy);

  ASSERT_NE(schema_copy.release, nullptr);
  EXPECT_STREQ(schema_copy.name, "field_name");
  EXPECT_NE(schema_copy.metadata, nullptr);

  auto int_field_roundtrip = ImportField(&schema_copy).ValueOrDie();
  EXPECT_EQ(int_field->name(), int_field_roundtrip->name());
  EXPECT_EQ(int_field_roundtrip->metadata()->Get("some_key").ValueOrDie(), "some_value");

  schema.release(&schema);
}
