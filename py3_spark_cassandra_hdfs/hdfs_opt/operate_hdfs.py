#!/usr/bin/env python3
# JetBrains PyCharm 2018.1.1

import os
import logging as logger
from .connect_hdfs import fs

__all__ = ['exists', 'cat', 'delete', 'rm', 'chmod', 'chown', 'info', 'ls',
           'mkdir', 'open', 'rename', 'download', 'upload', 'df', 'disk_usage',
           'get_capacity', 'get_space_used', 'get', 'put']


# judgement path is exist
def exists(path):
    """
    Returns True if the path is known to the cluster,
    False if it does not (or there is an RPC error)

    Parameters
    ----------
    path : HDFS path
    """
    try:
        return fs.exists(path)
    except Exception as e:
        logger.error(e)
        return False


# read file
def cat(path, encoding=None, is_print=False):
    """
    :param path: string,absolute path to file
    :param encoding: encoding, to decode bytes
    :param is_print: print file content
    :return: Return contents of file as a bytes object or None
              If None, fs.cat error
    """
    try:
        content = fs.cat(path)
        content = content if encoding is None else str(content, encoding)
        print(content) if is_print else None
    except Exception as e:
        logger.error(e)
        content = None
    return content


#  Delete the indicated file or directory
def delete(path, recursive=False):
    """
    Delete the indicated file or directory

    Parameters
    ----------
    path : string
    recursive : boolean, default False
        If True, also delete child paths for directories
    """
    try:
        fs.delete(path, recursive)
        status = True
    except Exception as e:
        logger.error(e)
        status = False if not exists(path) else True
    return status


# remove hdfs file
def rm(path, recursive=False):
    """
    Alias for FileSystem.delete
    """
    try:
        fs.rm(path, recursive)
        status = True
    except Exception as e:
        logger.error(e)
        status = False if not exists(path) else True
    return status


# Change file permissions
def chmod(path, mode):
    """
    Change file permissions

    Parameters
    ----------
    path : string
        absolute path to file or directory
    mode : int
        POSIX-like bitmask
        """
    fs.chmod(path, mode)


# Change file owner, group
def chown(path, owner=None, group=None):
    """
    Change file permissions

    Parameters
    ----------
    path : string
        absolute path to file or directory
    owner : string, default None
        New owner, None for no change
    group : string, default None
        New group, None for no change
    """
    fs.chown(path, owner, group)


# Return detailed HDFS information for path
def info(path):
    """
    Return detailed HDFS information for path

    Parameters
    ----------
    path (string) – Path to file or directory

    Returns
    -------
    path_info (dict)
    """
    try:
        return fs.info(path)
    except Exception as e:
        logger.error(e)
        return {}


# Retrieve directory contents and metadata, if requested.
def ls(path, detail=False):
    """
    Retrieve directory contents and metadata, if requested.

    Parameters
    ----------
    path : HDFS path
    detail : boolean, default False
        If False, only return list of paths

    Returns
    -------
    result : list of dicts (detail=True) or strings (detail=False)
    """
    try:
        return fs.ls(path, detail)
    except Exception as e:
        logger.error(e)
        return []


# Create directory in HDFS
def mkdir(path, **kwargs):
    """
    Create directory in HDFS

    Parameters
    ----------
    path : string
        Directory path to create, including any parent directories

    Notes
    -----
    libhdfs does not support create_parents=False, so we ignore this here
    """
    try:
        fs.mkdir(path, kwargs)
        status = True
    except Exception as e:
        logger.error(e)
        status = exists(path)
    return status


#  Open HDFS file for reading or writing
def open(path, mode='rb', buffer_size=None, replication=None,
         default_block_size=None):
    """
   Open HDFS file for reading or writing

   Parameters
   ----------
   path : string
        HDFS path
   mode : string
       Must be one of 'rb', 'wb', 'ab'
   buffer_size :
   replication :
   default_block_size:
   Returns
   -------
   handle : HdfsFile
   """
    return fs.open(path, mode, buffer_size, replication, default_block_size)


# Rename file, like UNIX mv command
def rename(path, new_path):
    """
    Rename file, like UNIX mv command

    Parameters
    ----------
    path : string
        Path to alter
    new_path : string
        Path to move to
    """
    fs.rename(path, new_path)


# download hdfs file to local
def download(dst_path, loc_path, buffer_size=None):
    """
    :param dst_path: hdfs file path
    :param loc_path: local path
    :param buffer_size:
    :return:
    """
    try:
        fs.download(dst_path, loc_path, buffer_size)
    except Exception as e:
        logger.error(e)


# upload local file to hdfs
def upload(dst_file_path, loc_file_path, buffer_size=None):
    """
    Upload file-like object to HDFS path
    """
    try:
        with open(loc_file_path, 'rb') as stream:
            fs.upload(dst_file_path, stream, buffer_size)
    except Exception as e:
        logger.error(e)


# Return free space on disk, like the UNIX df command
def df(option=None, decimal_place=2, is_print=None):
    """
    Return free space on disk, like the UNIX df command

    Parameters
    ----------
    option : default is None, others 'K', 'M', 'G', 'T'
    decimal_place: default is 2
    is_print: default None, don't print query df result

    Returns
    -------
    space : int, if option is None
    space : str, if option is 'K' or 'M' or 'G' or 'T'
    """
    unit_tuple = ('K', 'M', 'G', 'T')
    space = fs.df()
    if option and option in unit_tuple:
        space /= 1024 ** (unit_tuple.index(option) + 1)
        space = '{}{}'.format(round(space, decimal_place), option + 'b')
    print("free space: %s" % space) if is_print is not None else None
    return space


# Compute bytes used by all contents under indicated path in file tree
def disk_usage(path, option=None, decimal_place=2, is_print=None):
    """
    Compute bytes used by all contents under indicated path in file tree

    Parameters
    ----------
    path : string, Can be a file path or directory
    option : default is None, others 'K', 'M', 'G', 'T'
    decimal_place: default is 2
    is_print: default None, don't print query df result

    Returns
    -------
    usage : int
    usage: str, if option is 'K' or 'M' or 'G' or 'T'
    """
    usage = fs.disk_usage(path)
    unit_tuple = ('K', 'M', 'G', 'T')
    if option and option in unit_tuple:
        usage /= 1024 ** (unit_tuple.index(option) + 1)
        usage = '{}{}'.format(round(usage, decimal_place), option + 'b')
    print("used space: %s" % usage) if is_print is not None else None
    return usage


# Get reported total capacity of file system
def get_capacity(option=None, decimal_place=2, is_print=None):
    """
    Get reported total capacity of file system

    Parameters
    ----------
    option : default is None, others 'K', 'M', 'G', 'T'
    decimal_place: default is 2
    is_print: default None, don't print query df result

    Returns
    -------
    capacity : int
    capacity : str, if option is 'K' or 'M' or 'G' or 'T'
    """
    capacity = fs.get_capacity()
    unit_tuple = ('K', 'M', 'G', 'T')
    if option and option in unit_tuple:
        capacity /= 1024 ** (unit_tuple.index(option) + 1)
        capacity = '{}{}'.format(round(capacity, decimal_place), option + 'b')
    print("total capacity: %s" % capacity) if is_print is not None else None
    return capacity


# Get space used on file system
def get_space_used(option=None, decimal_place=2, is_print=None):
    """
    Get space used on file system

    Parameters
    ----------
    option : default is None, others 'K', 'M', 'G', 'T'
    decimal_place: default is 2
    is_print: default None, don't print query df result

    Returns
    -------
    space_used : int
    """
    unit_tuple = ('K', 'M', 'G', 'T')
    space_used = fs.get_space_used()
    if option and option in unit_tuple:
        space_used /= 1024 ** (unit_tuple.index(option) + 1)
        space_used = '{}{}'.format(round(space_used, decimal_place),
                                   option + 'b')
    print("used space: %s" % space_used) if is_print is not None else None
    return space_used


# extend function
# -------------------------------------
def _traverse_directory(loc_path):
    file_list = []
    for dirpath, dirnames, filenames in os.walk(loc_path):
        file_list += [os.path.join(dirpath, filename) for filename in filenames]
    return file_list


def _replace_path(loc_path, dst_path, rep_list):
    loc_path_dirname = os.path.dirname(loc_path)
    return [name.replace(loc_path_dirname, dst_path) for name in rep_list]


# upload local file or directory to hdfs
def put(loc_path, dst_path):
    if os.path.exists(loc_path) and os.path.isfile(loc_path):
        upload(dst_path, loc_path)
        return True

    if os.path.isdir(loc_path):
        file_list = _traverse_directory(loc_path)
        dst_file_list = _replace_path(loc_path, dst_path, file_list)
        map(lambda x: upload(*x), zip(dst_file_list, file_list))
    return False


# download hdfs file or directory to local
def get(dst_path, loc_path):
    pass


def _read_first_line(path, decode='utf-8', buffer_size=1000):
    with open(path, 'rb', buffer_size) as handle:
        first_line = b''
        while b'\n' not in first_line:
            first_line += handle.read(buffer_size)
        first_line = first_line.decode(decode).strip().split('/n')[0]
    return first_line


# read hdfs file head
def get_head(dst_path):
    header = {}
    if exists(dst_path):
        file_list = ls(dst_path)
        header = dict(map(lambda name: (os.path.basename(name),
                                        _read_first_line(name)), file_list))
    return header


if __name__ == "__main__":
    pass
