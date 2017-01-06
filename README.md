# bioio
Tools for improving throughput for bioinformatics data

Subdirectories
 - parse_nexus: parse NEXUS data files
 - zchunk: compress/decompress large files in parallel using MPI
 
Tools in this folder
 - extract: a Python script for extracting a subrange of a file. For example, the command "extract foo 2 5" will copy 5 bytes of the file "foo", starting at offset 2 (the third byte) to standard output.
 
    
