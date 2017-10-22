/*
  Unit tests for zlines.

  Ed Karrels, ed.karrels@gmail.com, January 2017
*/

#include <assert.h>
#include <string.h>
#include "zline_api.h"

#define FILENAME "test_zlines.out"

int test_add_one() {
  char *p, buf[100] = {0};
  ZlineFile *z;

  z = ZlineFile_create(FILENAME, 0);
  assert(ZlineFile_line_count(z) == 0);
  assert(0 == ZlineFile_max_line_length(z));
  
  ZlineFile_add_line(z, "foo", -1);
  assert(ZlineFile_line_count(z) == 1);

  assert(3 == ZlineFile_line_length(z, 0));
  assert(3 == ZlineFile_max_line_length(z));

  p = ZlineFile_get_line(z, 0, 0);
  assert(!strcmp(p, "foo"));
  free(p);

  ZlineFile_get_line(z, 0, buf);
  assert(!strcmp(buf, "foo"));

  assert(NULL == ZlineFile_get_line(z, 1, 0));
  assert(NULL == ZlineFile_get_line(z, 1000, 0));

  ZlineFile_close(z);

  z = ZlineFile_read(FILENAME);

  assert(z);
  assert(1 == ZlineFile_line_count(z));
  assert(3 == ZlineFile_line_length(z, 0));
  assert(3 == ZlineFile_max_line_length(z));
  
  p = ZlineFile_get_line(z, 0, 0);
  assert(!strcmp(p, "foo"));
  free(p);

  ZlineFile_get_line(z, 0, buf);
  assert(!strcmp(buf, "foo"));

  assert(NULL == ZlineFile_get_line(z, 1, 0));

  ZlineFile_close(z);

  return 0;
}
  

int main() {

  test_add_one();
  printf("ok\n");

  return 0;
}
