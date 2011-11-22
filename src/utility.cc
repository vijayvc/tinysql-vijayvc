/* File: utiliy.cc
 * ---------------
 * Implementation of simple printing functions to report failures or
 * debugging information triggered by keys.
 */

#include "utility.h"
#include <stdarg.h>

//#include <string.h>

static const int BufferSize = 2048;

void Failure(const char *format, ...)
{
  va_list args;
  char errbuf[BufferSize];
  
  va_start(args, format);
  vsprintf(errbuf, format, args);
  va_end(args);
  fflush(stdout);
  fprintf(stderr,"\n*** Failure: %s\n\n", errbuf);
  abort();
}
