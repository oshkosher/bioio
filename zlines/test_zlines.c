/*
  Unit tests for zlines.

  Ed Karrels, ed.karrels@gmail.com, January 2017
*/

#undef NDEBUG
#include <assert.h>
#include <string.h>
#include "zline_api.h"

#define FILENAME "test_zlines.out"

void test_add_one() {
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

  putchar('.'); fflush(stdout);
}


void test_add_some() {
  ZlineFile *z = ZlineFile_create(FILENAME, 0);
  char buf[100];

  assert(0 == ZlineFile_max_line_length(z));
  ZlineFile_add_line(z, "foo", -1);
  assert(3 == ZlineFile_max_line_length(z));
  ZlineFile_add_line(z, "bar", -1);
  assert(3 == ZlineFile_max_line_length(z));
  ZlineFile_add_line(z, "", 0);
  assert(3 == ZlineFile_max_line_length(z));
  ZlineFile_add_line(z, "gonzo", -1);
  assert(5 == ZlineFile_max_line_length(z));

  assert(4 == ZlineFile_line_count(z));
  assert(3 == ZlineFile_line_length(z, 0));
  assert(3 == ZlineFile_line_length(z, 1));
  assert(0 == ZlineFile_line_length(z, 2));
  assert(5 == ZlineFile_line_length(z, 3));

  assert(!strcmp("foo", ZlineFile_get_line(z, 0, buf)));
  assert(!strcmp("bar", ZlineFile_get_line(z, 1, buf)));
  assert(!strcmp("", ZlineFile_get_line(z, 2, buf)));
  assert(!strcmp("gonzo", ZlineFile_get_line(z, 3, buf)));

  ZlineFile_close(z);

  z = ZlineFile_read(FILENAME);
  assert(4 == ZlineFile_line_count(z));
  assert(5 == ZlineFile_max_line_length(z));
  assert(3 == ZlineFile_line_length(z, 0));
  assert(3 == ZlineFile_line_length(z, 1));
  assert(0 == ZlineFile_line_length(z, 2));
  assert(5 == ZlineFile_line_length(z, 3));

  assert(!strcmp("foo", ZlineFile_get_line(z, 0, buf)));
  assert(!strcmp("bar", ZlineFile_get_line(z, 1, buf)));
  assert(!strcmp("", ZlineFile_get_line(z, 2, buf)));
  assert(!strcmp("gonzo", ZlineFile_get_line(z, 3, buf)));

  ZlineFile_close(z);
  putchar('.'); fflush(stdout);
}


void test_blocks() {
  ZlineFile *z = ZlineFile_create(FILENAME, 100);
  char buf[100];

  ZlineFile_add_line(z, "this is 80 characters.......................................................done", -1);
  ZlineFile_add_line(z, "and here's 20*******", -1);

  ZlineFile_add_line(z, "one more", -1);

  assert(80 == ZlineFile_max_line_length(z));

  assert(!strcmp("one more", ZlineFile_get_line(z, 2, buf)));
  assert(!strcmp("and here's 20*******", ZlineFile_get_line(z, 1, buf)));
  assert(!strcmp("this is 80 characters.......................................................done", ZlineFile_get_line(z, 0, buf)));

  ZlineFile_close(z);

  z = ZlineFile_read(FILENAME);
  assert(80 == ZlineFile_max_line_length(z));

  assert(!strcmp("one more", ZlineFile_get_line(z, 2, buf)));
  assert(!strcmp("and here's 20*******", ZlineFile_get_line(z, 1, buf)));
  assert(!strcmp("this is 80 characters.......................................................done", ZlineFile_get_line(z, 0, buf)));
  assert(!strcmp("one more", ZlineFile_get_line(z, 2, buf)));

  ZlineFile_close(z);

  putchar('.'); fflush(stdout);
}


void test_long_line() {
  ZlineFile *z;
  const char *s1 = "this has 11";
  const char *s2 = "this is 50 chars..............................long";
  char buf[100];

  z = ZlineFile_create(FILENAME, 20);

  ZlineFile_add_line(z, s1, 11);
  ZlineFile_add_line(z, s2, 50);

  assert(!strcmp(s1, ZlineFile_get_line(z, 0, buf)));
  assert(!strcmp(s2, ZlineFile_get_line(z, 1, buf)));
  assert(!strcmp(s1, ZlineFile_get_line(z, 0, buf)));

  ZlineFile_close(z);

  z = ZlineFile_read(FILENAME);

  assert(!strcmp(s1, ZlineFile_get_line(z, 0, buf)));
  assert(!strcmp(s2, ZlineFile_get_line(z, 1, buf)));
  assert(!strcmp(s1, ZlineFile_get_line(z, 0, buf)));
  ZlineFile_close(z);
  
  putchar('.'); fflush(stdout);
}  


void test_many_lines() {
  char buf[100], buf2[100];
  int i, n = 1000;
  ZlineFile *z;

  z = ZlineFile_create(FILENAME, 0);

  for (i=0; i < n; i++) {
    sprintf(buf, "test line %10d", i);
    ZlineFile_add_line(z, buf, -1);
  }

  for (i=0; i < n; i++) {
    sprintf(buf2, "test line %10d", i);
    ZlineFile_get_line(z, i, buf);
    assert(!strcmp(buf, buf2));
  }

  ZlineFile_close(z);

  z = ZlineFile_read(FILENAME);

  for (i=0; i < n; i++) {
    sprintf(buf2, "test line %10d", i);
    ZlineFile_get_line(z, i, buf);
    assert(!strcmp(buf, buf2));
  }
  
  ZlineFile_close(z);

  putchar('.'); fflush(stdout);
}    
  


int main() {

  test_add_one();
  test_add_some();
  test_blocks();
  test_long_line();
  test_many_lines();
  
  remove(FILENAME);

  printf("ok\n");

  return 0;
}