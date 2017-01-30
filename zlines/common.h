#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdio.h>
#include <stdint.h>

/* If filename is "-", return stdin. Otherwise, open the given file
   for reading. */
FILE *openFileOrStdin(const char *filename);

uint64_t getFileSize(const char *filename);

/* If for_writing is nonzero, the file will be set to the length *length.
   Otherwise, *length will be set to the length of the file. */
int mapFile(const char *filename, int for_writing, char **data,
            uint64_t *length);


/* Formats a number with commas every 3 digits: 1234567 -> "1,234,567".
   The string will be written to buf (which must be at least 21 bytes),
   and buf will be returned.
*/
const char *commafy(char *buf, uint64_t n);

/* Return a relative time in seconds. */
double getSeconds();

/* Returns the amount of physical memory. */
uint64_t getMemorySize();

#endif /* __COMMON_H__ */
