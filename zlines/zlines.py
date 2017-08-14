#!/usr/bin/python

"""
Python wrapper for zlines API.

Sample usage:

  To create a compressed file:
  f = zlines.open('stuff.zlines', 'w')
  f.add('foo')
  f.add('bar')
  f.close()

  To read a compressed file:
  f = zlines.open('stuff.zlines')
  for line in f:
    print(line)

"""

import sys, platform
from ctypes import *

if platform.system()[:6] == 'CYGWIN':
  libname = 'libzlines.dll'
else:
  libname = 'libzlines.so'

# make sure zstd/lib/dll is in PATH or LD_LIBRARY_PATH
zlineslib = cdll.LoadLibrary(libname)


# clib = cdll.LoadLibrary('libc.so.6')
# c_free_fn = clib.free

# create a new file
ZlineFile_create = zlineslib.ZlineFile_create
ZlineFile_create.argtypes = [c_char_p, c_int]
ZlineFile_create.restype = c_void_p

# open an existing file
ZlineFile_read = zlineslib.ZlineFile_read
ZlineFile_read.restype = c_void_p

# add a line to a file being created
ZlineFile_add_line = zlineslib.ZlineFile_add_line
ZlineFile_add_line.argtypes = [c_void_p, c_char_p, c_ulonglong]
ZlineFile_add_line.restype = c_int

# get the number of lines in a file
ZlineFile_line_count = zlineslib.ZlineFile_line_count
ZlineFile_line_count.restype = c_ulonglong
ZlineFile_line_count.argtypes = [c_void_p]

# get the length of a given line
ZlineFile_line_length = zlineslib.ZlineFile_line_length
ZlineFile_line_length.argtypes = [c_void_p, c_ulonglong]
ZlineFile_line_length.restype = c_longlong

# get the maximum line length for this file
ZlineFile_max_line_length = zlineslib.ZlineFile_max_line_length
ZlineFile_max_line_length.argtypes = [c_void_p]
ZlineFile_max_line_length.restype = c_ulonglong

# get the contents for one line
ZlineFile_get_line = zlineslib.ZlineFile_get_line
ZlineFile_get_line.argtypes = [c_void_p, c_ulonglong, c_char_p]
ZlineFile_get_line.restype = c_char_p

# close a file (from either ZlineFile_create or ZlineFile_read)
ZlineFile_close = zlineslib.ZlineFile_close
ZlineFile_close.argtypes = [c_void_p]

class zline_file:
  def __init__(self, filename, mode='r', encoding='default'):
    """
    Open a zlines file for reading or writing.
    mode must be 'w' (create a new file) or 'r' (read an existing file).
    filename will be converted to UTF-8 encoding.

    encoding is the character set (None, ascii, utf-8, latin1, ...)
    that will be used to encode and decode strings read from or written
    to the file. If encoding is 'default', then under Python2 it will be
    None (no encoding change will be made) and under Python3 it will be
    'UTF-8'.

    Throws ValueError if mode is not 'w' or 'r'.
    Throws IOError if there is an error opening the file.
    """
    
    filename = filename.encode(encoding='UTF-8')
    
    if encoding == 'default':
      if sys.version_info.major < 3:
        encoding = None
      else:
        encoding = 'UTF-8'
    self._encoding = encoding

    self._file = None
    
    if mode == 'w':
      self._file = ZlineFile_create(filename, 4*1024*1024)
      if self._file == None:
        raise IOError('Cannot create file')
    elif mode == 'r':
      self._file = ZlineFile_read(filename)
      if self._file == None:
        raise IOError('Cannot read file or incorrect format')
    else:
      raise ValueError('Invalid mode ' + repr(mode))

    
  def close(self):
    """Closes the file"""
    if self._file:
      ZlineFile_close(self._file)
      self._file = None
      

  def __del__(self):
    self.close()
    

  def add(self, line):
    """
    If the file has been opened for writing, add the given line to it.
    Otherwise, throw a RuntimeError.
    """
    if self._encoding:
      line = line.encode(self._encoding)

    result = ZlineFile_add_line(self._file, line, 0)
    if result == -1:
      raise RuntimeError('File is opened for reading')


  def __len__(self):
    """
    Returns the number of lines in the file.
    """
    return int(ZlineFile_line_count(self._file))


  def line_len(self, line_no):
    """
    Returns the length of the given line.
    If line_no > len(self), returns -1.
    """
    return int(ZlineFile_line_length(self._file, line_no))


  def max_line_len(self):
    """
    Returns the length of the longest line in the file.
    """
    return int(ZlineFile_max_line_length(self._file))


  def __getitem__(self, line_no):
    """
    Returns the line_no'th line from the file, where the first line is zero.
    Raises IndexError if line_no < 0 or line_no >= len(self).
    """

    nlines = len(self)
    
    # handle slices
    if isinstance(line_no, slice):
      result = []
      start = 0 if line_no.start==None else line_no.start
      stop  = nlines if (line_no.stop==None or line_no.stop >= nlines) else line_no.stop
      step  = 1 if line_no.step==None else line_no.step
      for index in range(start, stop, step):
        try:
          result.append(self[index])
        except IndexError:
          pass
      return result
    
    # implement negative indices
    if line_no < 0:
      line_no += nlines
    
    llen = self.line_len(line_no)
    if llen == -1: raise IndexError
    
    buf = create_string_buffer(llen+1)
    line = ZlineFile_get_line(self._file, line_no, buf)
    if line == None: return None
    if self._encoding:
      return line.decode(self._encoding)
    else:
      return line


def open(filename, mode='r', encoding='default'):
  return zline_file(filename, mode, encoding)
