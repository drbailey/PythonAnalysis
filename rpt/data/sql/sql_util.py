__author__  = 'drew bailey'
__version__ = 1.0

"""

"""

import sqlite3
import os


def is_db(database, table=None, memory=False):
    """
    Checks if database or database.table exists in cwd.
    """
    path = './%s' % database
    if memory:
        return __sub_is_db(database, table)
    elif os.path.isfile(path) and os.access(path, os.R_OK):
        return __sub_is_db(database, table)
    else:
        return False


def __sub_is_db(database, table):
    conn = sqlite3.connect('%s' % database)
    curs = conn.cursor()
    if table:
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" % table
        d = curs.execute(sql).fetchone()
        curs.close()
        conn.close()
        if d:
            return True
        else:
            return False
    else:
        return True