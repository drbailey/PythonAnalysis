__author__  = 'drew bailey'
__version__ = 1.0

"""
Handles file distribution to PATH locations.
Should try to look as much like an email distribution as possible.
Uses shutil package.
"""

from ..data import Table
from ..util import Path
import sqlite3
import pyodbc
import shutil
import os


def rwc_file(path, filename=None, rwc_mode='r', data=None):
    """
    Handles reading, writing, and copying files and file information to any accessible PATH.
    :param path: desired PATH
    :param filename: file to read, auto_write, or copy
    :param rwc_mode: any valid read/auto_write mode, or 'c' or 'cb' for copy without meta, 'c+' or 'cm' with meta
    :param data: data to auto_write if applicable
    :return: data as mimic if mode is read, else nothing
    """
    if not os.path.exists(path):
        os.makedirs(path)
    if rwc_mode in ['c', 'cb']:
        shutil.copy(filename, path)
    elif rwc_mode in ['c+', 'cm']:
        shutil.copy2(filename, path)
    else:
        with Path(new=path):
            with open(filename, rwc_mode) as f:
                if rwc_mode in ['r', 'rb', 'r+']:
                    rdata = f.readlines()
                    return Table(children=rdata)
                elif rwc_mode in ['w', 'w+', 'wb', 'wb+', 'a', 'a+', 'ab', 'ab+']:
                    if hasattr(data, '__iter__') or type(data) in [sqlite3.Row, pyodbc.Row]:
                        for line in data:
                            f.write(line)
                    else:
                        f.write(data)
                else:
                    pass
    return