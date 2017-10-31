# zlines
A tool for storing a large number of text lines in a compressed file and an API for accessing those lines efficiently

This uses the Zstandard algorithm developed by Facebook: https://github.com/facebook/zstd
 
Sample command lines

    ./zlines create large_file.zlines large_file.txt
    ./zlines verify large_file.zlines large_file.txt
    gunzip < large_file.txt.gz | ./zlines create large_file.zlines -
    
    ./zlines get large_file.zlines 0 100 1000 10000:11000 -10:
    
To extract character data from a compressed NEXUS file and put it in a zlines file:

    gunzip < bigfile.nex.gz 2>/dev/null | ../parse_nexus/nexus_chars - | ./zlines create bigfile.chars.zlines -

C interface
 - See zlines_test.c for sample usage
 - See zline_api.h for a description of each function

Python interface
 - Run "pysetup" to see the commands needed to add this module to your
   system so you can access the module from other directories. (There
   is probably a better way of doing this with a Python package.)
 - See the comment at the top of zlines.py

Files
 - zlines.c - command line tool for creating a compressed file, plus options to verify it, show internal details, or extract a few lines
 - zline_api.c / zline_api.h - the C API for accessing one of these files
 - zlines_test.c - example of how to use the API to read from a zlines compressed file

File format
 - 256 byte text header containing a version number, locations of the data and index, number of text lines, number of compressed blocks, and other settings
 - Compressed blocks of data. Each block contains an index of the lines in the block, followed by the compressed contents of all the lines in the block.
 - 0-7 pad bytes, so the block index is 8-byte aligned
 - length of the compressed index
 - length of the block starts index
 - compressed block index
 - compressed block starts array (the first line in each block)

When a file is opened for reading, the header, block index, and block starts array are read in. These are small and can be read quickly.
For example, with a sample 5GB file containing 244 million lines, these sections of the file add up to just 47078 bytes. When a line is requested, the block containing that line is read, decompressed, and the line is copied from it. The most recently decompressed block is kept in memory, so if another line is requested from the same block, it can be retrieved without reading from the file.

