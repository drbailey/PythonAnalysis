__author__  = 'drew bailey'
__version__ = 0.1


"""
Master path file backup methods.
"""


from .distribution import rwc_file
from .config import MASTER_PATH, BACKUP_PATH
from .util import broadcast
from .data import BACKENDS, MASTER, MASTER_MEMORY
import os
import re


# TODO: define this backup path through config. Built very quickly, check for bugs.
def backup_templates(path=u'C:\\Users\\dbai00o\\Desktop\\Backup\\Reports\\Template Backups\\'):
    """
    Walks master path and saves any files including the Template pattern string to a backup folder outside master directory.
    :return: None
    """
    pattern = '^Template_.+\.xls\w*'
    __backup(path=path, pattern=pattern)


def backup_databases(path=u'C:\\Users\\dbai00o\\Desktop\\Backup\\Reports\\Database Backups\\'):
    """
    Walks master path and saves any files including the Database pattern string to a backup folder outside master directory.
    :return: None
    """
    # TODO: SQLite3 databases may be other extensions. Find a good way of determining if targets are database files.
    pattern = '^.+\.db'
    __backup(path=path, pattern=pattern)


def __backup(path, pattern):
    """
    Walks master path and saves any files including the pattern string to a backup folder outside master directory.
    :return: None
    """
    broadcast(msg='Backing up Templates...', clamor=1)
    if not path:
        path = os.path.dirname(BACKUP_PATH)

    for root, dirs, files in os.walk(path):
        for f in files:
            match = re.search(pattern, f)
            if match:
                broadcast(msg='{0: <95}\t{1:<}'.format(root, f), clamor=5)
                rwc_file(path=path, filename=root+'\\'+f, rwc_mode='c+')
    broadcast(msg='Backup Complete.', clamor=1)


def delete_data(path=u'C:\\Users\\dbai00o\\Desktop\\Backup\\Reports\\Database Backups\\'):
    pattern = ''
    __delete(path=path, pattern=pattern)


def delete_shards(path=u'C:\\Users\\dbai00o\\Desktop\\Backup\\Reports\\Database Backups\\'):
    pattern = ''
    __delete(path=path, pattern=pattern)


def __delete(path, pattern):
    """
    Walks master path and deletes any files including the pattern string.
    :return: None
    """
    broadcast(msg='Backing up Templates...', clamor=1)
    if not path:
        path = os.path.dirname(BACKUP_PATH)

    for root, dirs, files in os.walk(path):
        for f in files:
            match = re.search(pattern, f)
            if match:
                broadcast(msg='{0: <95}\t{1:<}'.format(root, f), clamor=5)
                try:
                    os.remove('')
                except WindowsError:
                    broadcast(msg='WindowsError occurred: file %s not deleted. (File is likely in use).' % f, clamor=0)
    broadcast(msg='Delete Complete.', clamor=1)


def drop_temp_tables():
    for connect in (MASTER, MASTER_MEMORY):
        table_names = BACKENDS.get_table_names(connect)
        for table in table_names:
            if '_temp' in table:
                BACKENDS.drop_table(connect=connect, table=table)
