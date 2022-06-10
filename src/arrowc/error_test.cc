
#include <cstring>
#include <string>

#include <gtest/gtest.h>

#include "arrowc/arrowc.h"

TEST(ErrorTest, ErrorTestSet) {
  ArrowError error;
  EXPECT_EQ(ArrowErrorSet(&error, "there were %d foxes", 4), ARROWC_OK);
  EXPECT_STREQ(ArrowErrorMessage(&error), "there were 4 foxes");
}

TEST(ErrorTest, ErrorTestSetOverrun) {
  ArrowError error;
  char big_error[2048];
  const char* a_few_chars = "abcdefg";
  for (int i = 0; i < 2047; i++) {
    big_error[i] = a_few_chars[i % strlen(a_few_chars)];
  }
  big_error[2047] = '\0';

  EXPECT_EQ(ArrowErrorSet(&error, "%s", big_error), ARROWC_OK);

  EXPECT_EQ(std::string(ArrowErrorMessage(&error)), std::string(big_error, 1022));
}
