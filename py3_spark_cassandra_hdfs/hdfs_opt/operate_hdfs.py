#!/usr/bin/env python3
# JetBrains PyCharm 2018.1.1

import logging as logger
from .connect_hdfs import fs

__all__ = ['exists', 'cat', 'delete', 'rm', 'chmod', 'chown', 'info', 'ls',
           'mkdir', 'open', 'rename', 'download', 'upload', 'df', 'disk_usage',
           'get_capacity', 'get_space_used']


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
    fs.mkdir(path, kwargs)


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
    fs.download(dst_path, loc_path, buffer_size)


def upload(path, stream, buffer_size=None):
    """
    Upload file-like object to HDFS path
    """
    fs.upload(path, stream, buffer_size)


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


if __name__ == "__main__":
    pass
