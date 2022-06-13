
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
