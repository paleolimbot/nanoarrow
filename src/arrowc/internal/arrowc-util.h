
#pragma once

#define ARROWC_RETURN_NOT_OK(expr) \
  if ((result = expr) != 0) return result;
