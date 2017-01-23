# zlines
A tool for storing a large number of text lines in a compressed file and an API for accessing those lines efficiently

This uses the Zstandard algorithm developed by Facebook: https://github.com/facebook/zstd

File format
 - 256 byte text header containing a version number, locations of the data and index, number of text lines, number of compressed blocks, and other settings
 - compressed blocks of data. By default, 4 megabytes of data are compressed into each block (can be changed with the -b option to zlines create)
 - 0-7 pad bytes, so the index is 8-byte aligned
 - if the index is not compressed:
   - block array with an entry for each compressed block: offset in the file, original length, and compressed length
   - line array with an entry for each line of text: block index, offset, and original length
 - if the index is compressed:
   - length of the compressed block array
   - length of the compressed line array
   - compressed block array
   - compressed line array

Files
 - zlines.c - command line tool for creating a compressed file, plus options to verify it, show internal details, or extract a few lines
 - zlines_api.c / zlines_api.h - the C API for accessing one of these files
 - zlines_test.c - example of how to use the API to read from a zlines compressed file
 
Sample command lines

    ./zlines create large_file.txt large_file.zlines
    ./zlines verify large_file.txt large_file.zlines
    gzip < large_file.txt.gz | ./zlines create - large_file.zlines
    
    ./zlines get large_file.zlines 0 100 1000
    
To extract character data from a compressed NEXUS file and put it in a zlines file:

    gunzip < bigfile.nex.gz 2>/dev/null | ../parse_nexus/nexus_chars - | ./zlines create - bigfile.chars.zlines

