/*
  zlines

  A tool for storing a large number of text lines in a compressed file
  and an API for accessing those lines efficiently.


  https://github.com/oshkosher/bioio/tree/master/zlines

  Ed Karrels, ed.karrels@gmail.com, January 2017
*/

#ifndef __ZLINE_API_H__
#define __ZLINE_API_H__

#include <stdint.h>

typedef struct {
  int placeholder;
} ZlineFile;



#ifdef __CYGWIN__
#define ZLINE_EXPORT __attribute__ ((visibility ("default")))
#elif defined(_WIN32)
#define ZLINE_EXPORT __declspec(dllexport)
#else
#define ZLINE_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Creates a new zlines file using blocks of the default size
   (currently 4 MiB).

   Use the result as the 'zf' argument to other functions in this
   module.

   Call ZlineFile_close(zf) to finish writing the file and close it.
*/
ZLINE_EXPORT ZlineFile *ZlineFile_create(const char *filename);

  
/* Like ZlineFile_create, but the user can select the block size. */
ZLINE_EXPORT ZlineFile *ZlineFile_create2(const char *filename,
                                          uint64_t block_size);

  
/* Open an existing zlines file for reading.
   Use the result as the 'zf' argument to other functions in this module.
   Call ZlineFile_close(zf) to close the file.
*/
ZLINE_EXPORT ZlineFile *ZlineFile_read(const char *filename);


/* If the file is open for writing, this finishes writing the file.
   The file is closed, and any memory allocated internally is deallocated. */
ZLINE_EXPORT void ZlineFile_close(ZlineFile *zf);

  
/* Adds a line of text to the file.
   Returns -1 if the file is opened for reading, or 0 on success.
*/
ZLINE_EXPORT int ZlineFile_add_line(ZlineFile *zf, const char *line);

  
/* Like ZlineFile_add_line, but the length of the line is supplied. */
ZLINE_EXPORT int ZlineFile_add_line2(ZlineFile *zf, const char *line,
                                     uint64_t length);

  
/* Returns the number of lines in the file. */
ZLINE_EXPORT uint64_t ZlineFile_line_count(ZlineFile *zf);

  
/* Returns the length of the given line or -1 if there is no such line. */
ZLINE_EXPORT int64_t ZlineFile_line_length(ZlineFile *zf, uint64_t line_idx);

  
/* Returns the length of the longest line. */
ZLINE_EXPORT uint64_t ZlineFile_max_line_length(ZlineFile *zf);

  
/* Reads a line from the file and returns it as a string.
   The result has been allocated with malloc(); the caller is responsible
   for deallocating it with free().

   If line_idx is invalid, a memory allocation failed, or an error was
   encountered reading the file, this will return NULL.
*/
ZLINE_EXPORT char *ZlineFile_get_line(ZlineFile *zf, uint64_t line_idx);

  
/* Like ZlineFile_get_line, but rather than copying the whole line, it
   copies part of line starting 'offset' bytes from its beginning.

   The results are written to 'buf', including a nul terminating
   byte. If the results are longer than buf_len bytes, only the first
   'buf_len'-1 bytes and a nul terminator are copied to 'buf'.

   Returns NULL on error, otherwise it returns 'buf'.
*/
ZLINE_EXPORT char *ZlineFile_get_line2
  (ZlineFile *zf, uint64_t line_idx,
   char *buf, uint64_t buf_len, uint64_t offset);



/* The functions below are only useful for looking inside the implementation. */


  
/* Returns the number of compressed blocks in the file.
   If the file is open in write mode, this may under-report the block
   count by one, because it won't count a block to which data is still
   being added. */
ZLINE_EXPORT uint64_t ZlineFile_get_block_count(ZlineFile *zf);

/* Returns the compressed or decompressed size of the given block.
   If block_idx is invalid, returns 0. */
ZLINE_EXPORT uint64_t ZlineFile_get_block_size_original
  (ZlineFile *zf, uint64_t block_idx);
ZLINE_EXPORT uint64_t ZlineFile_get_block_size_compressed
  (ZlineFile *zf, uint64_t block_idx);
ZLINE_EXPORT uint64_t ZlineFile_get_block_first_line
  (ZlineFile *zf, uint64_t block_idx);
ZLINE_EXPORT uint64_t ZlineFile_get_block_line_count
  (ZlineFile *zf, uint64_t block_idx);
ZLINE_EXPORT int ZlineFile_get_line_details
  (ZlineFile *zf, uint64_t line_idx, uint64_t *length,
   uint64_t *offset_in_block, uint64_t *block_idx);
/* Return the offset in the file where the data for this block is stored */
ZLINE_EXPORT uint64_t ZlineFile_get_block_offset
  (ZlineFile *zf, uint64_t block_idx);
/* Return the offset in the file where the block index starts */
ZLINE_EXPORT uint64_t ZlineFile_get_block_index_offset(ZlineFile *zf);



 
#ifdef __cplusplus
}
#endif



#endif /* __ZLINE_API_H__ */
