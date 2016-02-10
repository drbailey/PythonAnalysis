__author__ = 'drew bailey'
__version__ = 2.0

"""
__init__ for sql package.
Internal sql should not be passed outside of data package as these are not intended to be user facing.
"""

from ...config import SQL_CLEAN_LEVEL, MASTER_DB_NAME, MASTER_PATH, MASTER_TEXT_FACTORY
from sql_backends import (SQLBackends, SQLiteBackends, ODBCBackends, MSSQLServerBackends, OracleBackends,
                          TeradataBackends, IBMDB2Backends)
from sql_util import is_db
import sqlite3


__all__ = ['__author__',
           '__version__',
           'SQLBackends',
           'SQLiteBackends',
           'ODBCBackends',
           'MSSQLServerBackends',
           'OracleBackends',
           'TeradataBackends',
           'IBMDB2Backends',
           'is_db',
           'BACKENDS',
           'MASTER',
           'MASTER_MEMORY',
           ]

# ## SQLBackends INSTANCE ###
BACKENDS = SQLiteBackends()
BACKENDS.clean_level = SQL_CLEAN_LEVEL

# ## Master Connection ###
master_con = sqlite3.connect(MASTER_PATH+'\\'+MASTER_DB_NAME)
master_con.text_factory = MASTER_TEXT_FACTORY
master_crs = master_con.cursor()
MASTER = (master_con, master_crs)

# ## Master Memory Connection ###
master_memory_con = sqlite3.connect(':memory:')
master_memory_con.text_factory = MASTER_TEXT_FACTORY
master_memory_crs = master_memory_con.cursor()
MASTER_MEMORY = (master_memory_con, master_memory_crs)
