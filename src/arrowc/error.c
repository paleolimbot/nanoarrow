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

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "arrowc.h"

int ArrowErrorSet(struct ArrowError* error, const char* fmt, ...) {
  memset(error->message, 0, 1024);

  va_list args;
  va_start(args, fmt);
  int result = vsnprintf(error->message, 1024 - 1, fmt, args);
  va_end(args);

  return 0;
}

const char* ArrowErrorMessage(struct ArrowError* error) {
  return error->message;
}
