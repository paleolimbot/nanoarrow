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
#include <arrow/memory_pool.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/key_value_metadata.h>

#include "arrowc/arrowc.h"

using namespace arrow;

TEST(SchemaTest, SchemaAllocate) {
  struct ArrowSchema schema;
  ArrowSchemaInit(2, &schema);

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
