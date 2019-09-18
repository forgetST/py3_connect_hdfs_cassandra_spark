#!/usr/bin/env python3
# JetBrains PyCharm 2018.1.1

from .connect_hdfs import fs


def cat(path):
    """
    :param path: string,absolute path to file or directory
    :return: Return contents of file as a bytes object
    """
    fs.cat(path)


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


def delete(path, recursive=False):
    """
    Delete the indicated file or directory

    Parameters
    ----------
    path : string
    recursive : boolean, default False
        If True, also delete child paths for directories
    """
    fs.delete(path, recursive)


def df():
    """
    Return free space on disk, like the UNIX df command

    Returns
    -------
    space : int
    """
    fs.df()


def disk_usage(path):
    """
    Compute bytes used by all contents under indicated path in file tree

    Parameters
    ----------
    path : string
        Can be a file path or directory

    Returns
    -------
    usage : int
    """
    fs.disk_usage(path)


def exists(path):
    """
    Returns True if the path is known to the cluster,
    False if it does not (or there is an RPC error)

    Parameters
    ----------
    path : HDFS path
    """
    fs.exists(path)


def get_capacity():
    """
    Get reported total capacity of file system

    Returns
    -------
    capacity : int
    """
    fs.get_capacity()


def get_space_used():
    """
    Get space used on file system

    Returns
    -------
    space_used : int
    """
    fs.get_space_used()


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
    fs.info(path)


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
    fs.ls(path, detail)


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


def opens(path, mode='rb', buffer_size=None, replication=None,
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
    fs.open(path, mode, buffer_size, replication, default_block_size)


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


def rm(path, recursive=False):
    """
    Alias for FileSystem.delete
    """
    fs.rm(path, recursive)


def download(path, stream, buffer_size=None):
    fs.download(path, stream, buffer_size)


def upload(path, stream, buffer_size=None):
    """
    Upload file-like object to HDFS path
    """
    fs.upload(path, stream, buffer_size)
