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

#include "arrowc/arrowc.h"

using namespace arrow;

TEST(SchemaViewTest, SchemaViewInitErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

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

  schema.release(&schema);
}
