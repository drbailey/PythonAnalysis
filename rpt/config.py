__author__  = 'drew bailey'
__version__ = 1.0

"""
Package wide variables.
"""

from .path import PATH, BACKUP_PATH
import os

__all__ = ['__author__',
           '__version__',
           'SQL_CLEAN_LEVEL',
           'MIN_PRINT_LENGTH',
           'MAX_PRINT_LENGTH',
           'MAX_PRINT_ROWS',
           'MAX_PRINT_COLUMNS',
           'MASTER_PATH',
           'MASTER_DB_NAME',
           'GLOBAL_TABLE',
           'GLOBAL_FIELDS',
           'GLOBAL_RESERVED',
           'CONNECTION_TABLE',
           'CONNECTION_FIELDS',
           'USER_TABLE',
           'USER_FIELDS',
           'CRON_TABLE',
           'CRON_FIELDS',
           'CRON_LOG_TABLE',
           'CRON_LOG_FIELDS',
           'LOG_TABLE',
           'LOG_FIELDS',
           'MASTER_TABLES',
           'DATE_FORMAT',
           'SQL_RESERVED',
           'SQL_REPLACE',
           'EXTENSION_PRIORITY',
           'COM_ZERO_INDEXED',
           'VOLUME',
           'VOLUME_DOCS',
           ]


VOLUME = 7  # 0 is print nothing, higher is printing more.
VOLUME_DOCS = """
VOLUME levels follow these guidelines:

0   nothing
1   potentially fatal high-level information and warnings
2   high-level information and warnings
3   potentially fatal mid-level information and warnings
4   mid-level information and warnings
5   potentially fatal low-level information and warnings
6   low-level information and warnings
7   high-level debug
8   mid-level debug
9   low-level debug
10  everything

package levels follow these guidelines:

high-level  critical directly user-facing classes and functions
mid-level   critical non-user-facing classes and functions or high-level sub functions
low-level   non-critical classes and functions

user-facing classes and functions are defined as:

a)  a function intended to be directly called by a user
b)  an interface that is interacted with by the user
c)  a function that returns something ultimately seen by a user (piece of data, email message, etc...)

"""

# ## GLOBAL DATABASE VARIABLES ###
SQL_CLEAN_LEVEL = 1
# NESTED_NULL = [None, False, [], (), [()], [None, ], (None, ), [(None), ]]

# ## GLOBAL OBJECT STR VARIABLES LENGTHS ###
MIN_PRINT_LENGTH = 14
MAX_PRINT_LENGTH = 20
MAX_PRINT_ROWS = 40
MAX_PRINT_COLUMNS = 20  # determine off shell width?


# ## GLOBAL MASTER/SETTINGS DATABASE VARIABLES ###
if not PATH:
    import getpass
    PATH = r"C:\Users\%s\Documents\RPT" % getpass.getuser()

if not BACKUP_PATH:
    BACKUP_PATH = PATH

if not os.path.exists(PATH):
    os.makedirs(PATH)

if not os.path.exists(BACKUP_PATH):
    os.makedirs(BACKUP_PATH)

MASTER_PATH = PATH
PACKAGE_PATH = os.path.dirname(os.path.realpath(__file__))

MASTER_DB_NAME = 'Master.db'
MASTER_TEXT_FACTORY = str

GLOBAL_TABLE = 'GLOBAL_SETTINGS'
GLOBAL_FIELDS = [
    'GLOBAL_ID',
    'TYPE',
    'FULL',
    'SERVER',
    'PORT',
    ]
GLOBAL_TYPES = [
    'INTEGER PRIMARY KEY AUTOINCREMENT',
    'TEXT',
    'TEXT',
    'TEXT',
    'INTEGER',
    ]

GLOBAL_RESERVED = ['http', 'ftp', 'smtp', 'view']

USER_TABLE = 'USER_SETTINGS'
USER_FIELDS = [
    'USER_ID',
    'USER_NAME',
    'DEFAULT_DATABASE',
    'DEFAULT_CONNECTION_STRING',
    'DEFAULT_EMAIL',
    ]
USER_TYPES = [
    'INTEGER PRIMARY KEY AUTOINCREMENT',
    'TEXT',
    'TEXT',
    'TEXT',
    'TEXT',
    ]

CONNECTION_TABLE = 'CONNECTION_SETTINGS'
CONNECTION_FIELDS = [
    'CONNECTION_ID',
    'USER_NAME',
    'DATABASE',
    'CONNECTION_STRING',
    ]
CONNECTION_TYPES = [
    'INTEGER PRIMARY KEY AUTOINCREMENT',
    'TEXT',
    'TEXT',
    'TEXT',
    ]

CRON_TABLE = 'CRON'
CRON_FIELDS = [
    'CRON_ID',
    'TASK_NAME',
    'USER_NAME',
    'NEXT_RUN',
    'LAST_RUN',
    'DELTA',
    'LOAD_DT',
    'RUN_END_DT',
    'FLAGGED_RUNS',
    'PRIORITY',
    'DELAY',
    ]
CRON_TYPES = [
    'INTEGER PRIMARY KEY AUTOINCREMENT',
    'TEXT',
    'TEXT',
    'DATETIME',
    'DATETIME',
    'INTEGER',
    'DATETIME',
    'DATETIME',
    'NONE',
    'REAL',
    'REAL',
    ]

CRON_LOG_TABLE = 'CRON_LOG'
CRON_LOG_FIELDS = [
    'CRON_LOG_ID',
    'CRON_ID',
    'CREATED',
    'NAME',
    'MESSAGE',
    'ELAPSED',
    'RESULT',
    'PROCESS',
    'PROCESS_NAME',
    'THREAD',
    'THREAD_NAME',
    'EXCEPTION',
    'LINE_NO',
    ]  # not used create table
CRON_LOG_TYPES = [
    'INTEGER PRIMARY KEY AUTOINCREMENT',
    'INTEGER',
    'DATETIME',
    'TEXT',
    'TEXT',
    'REAL',
    'TEXT',
    'INTEGER',
    'TEXT',
    'INTEGER',
    'TEXT',
    'TEXT',
    'INTEGER',
    ]

LOG_TABLE = 'LOG'
LOG_FIELDS = [
    'LOG_ID',
    'CREATED',
    'NAME',
    'LOG_LEVEL',
    'LOG_LEVEL_NAME',
    'MESSAGE',
    'ARGS',
    'MODULE',
    'FUNC_NAME',
    'PROCESS',
    'PROCESS_NAME',
    'THREAD',
    'THREAD_NAME',
    'EXCEPTION',
    'LINE_NO',
    ]  # not used in create table
LOG_TYPES = [
    'INTEGER PRIMARY KEY AUTOINCREMENT',
    'DATETIME',
    'TEXT',
    'INTEGER',
    'TEXT',
    'TEXT',
    'TEXT',
    'TEXT',
    'TEXT',
    'INTEGER',
    'TEXT',
    'INTEGER',
    'TEXT',
    'TEXT',
    'INTEGER',
    ]

MASTER_TABLES = [GLOBAL_TABLE, USER_TABLE, CONNECTION_TABLE, CRON_TABLE, LOG_TABLE, CRON_LOG_TABLE]
# table: [index_is_unique, fields]
MASTER_TABLES_INDICES = {GLOBAL_TABLE: [True, 'TYPE'],
                         USER_TABLE: [True, 'USER_NAME'],
                         CONNECTION_TABLE: [True, 'USER_NAME', 'DATABASE'],
                         CRON_TABLE: [True, 'USER_NAME', 'TASK_NAME'],
                         LOG_TABLE: [False, 'Created'],
                         CRON_LOG_TABLE: [False, 'Created'],
                         }

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FOLDER_FORMAT = "%Y.%m.%d"
DATE_FORMAT_SHORT = "%Y-%m-%d"
DATE_FORMAT_REGEX = "(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})"
DATE_FORMAT_REGEX_SHORT = "(\d{4})-(\d{2})-(\d{2})"

EXTENSION_PRIORITY = 'abcdefghijklmnopqrstuvwxyz'

DATA_ENCODING = 'ascii'

COM_ZERO_INDEXED = False


# ## ERROR MESSAGING ###
CHILD_TYPE_ERROR = '%s is not an allowed member class. Members required to be a class in %s.'
CHILD_KEY_ERROR = "'%s' is not a valid reference for this object. " \
                  "Please use an attribute name in dot notation, an item name in dot or bracket notation, " \
                  "or an item index integer in bracket notation."
CHILD_RESERVED_ERROR = "'%s' is a reserved attribute name. Please select a different key name."
PARENT_TYPE_ERROR = '%s is not an allowed parent class. Parent required to be a class in %s.'

SQL_REPLACE = {'\)': '',
               '\(': '',
               "'": "''",
               '"': '',
               '-': '_',
               ' ': '_',
               '&': 'and',
               ';': '',
               ':': ''}

SQL_RESERVED = [
    'ADD',
    'EXTERNAL',
    'PROCEDURE',
    'ALL',
    'FETCH',
    'PUBLIC',
    'ALTER',
    'FILE',
    'RAISERROR',
    'AND',
    'FILLFACTOR',
    'READ',
    'ANY',
    'FOR',
    'READTEXT',
    'AS',
    'FOREIGN',
    'RECONFIGURE',
    'ASC',
    'FREETEXT',
    'REFERENCES',
    'AUTHORIZATION',
    'FREETEXTTABLE',
    'REPLICATION',
    'BACKUP',
    'FROM',
    'RESTORE',
    'BEGIN',
    'FULL',
    'RESTRICT',
    'BETWEEN',
    'FUNCTION',
    'RETURN',
    'BREAK',
    'GOTO',
    'REVERT',
    'BROWSE',
    'GRANT',
    'REVOKE',
    'BULK',
    'GROUP',
    'RIGHT',
    'BY',
    'HAVING',
    'ROLLBACK',
    'CASCADE',
    'HOLDLOCK',
    'ROWCOUNT',
    'CASE',
    'IDENTITY',
    'ROWGUIDCOL',
    'CHECK',
    'IDENTITY_INSERT',
    'RULE',
    'CHECKPOINT',
    'IDENTITYCOL',
    'SAVE',
    'CLOSE',
    'IF',
    'SCHEMA',
    'CLUSTERED',
    'IN',
    'SECURITYAUDIT',
    'COALESCE',
    'INDEX',
    'SELECT',
    'COLLATE',
    'INNER',
    'SEMANTICKEYPHRASETABLE',
    'COLUMN',
    'INSERT',
    'SEMANTICSIMILARITYDETAILSTABLE',
    'COMMIT',
    'INTERSECT',
    'SEMANTICSIMILARITYTABLE',
    'COMPUTE',
    'INTO',
    'SESSION_USER',
    'CONSTRAINT',
    'IS',
    'SET',
    'CONTAINS',
    'JOIN',
    'SETUSER',
    'CONTAINSTABLE',
    'KEY',
    'SHUTDOWN',
    'CONTINUE',
    'KILL',
    'SOME',
    'CONVERT',
    'LEFT',
    'STATISTICS',
    'CREATE',
    'LIKE',
    'SYSTEM_USER',
    'CROSS',
    'LINENO',
    'TABLE',
    'CURRENT',
    'LOAD',
    'TABLESAMPLE',
    'CURRENT_DATE',
    'MERGE',
    'TEXTSIZE',
    'CURRENT_TIME',
    'NATIONAL',
    'THEN',
    'CURRENT_TIMESTAMP',
    'NOCHECK',
    'TO',
    'CURRENT_USER',
    'NONCLUSTERED',
    'TOP',
    'CURSOR',
    'NOT',
    'TRAN',
    'DATABASE',
    'NULL',
    'TRANSACTION',
    'DBCC',
    'NULLIF',
    'TRIGGER',
    'DEALLOCATE',
    'OF',
    'TRUNCATE',
    'DECLARE',
    'OFF',
    'TRY_CONVERT',
    'DEFAULT',
    'OFFSETS',
    'TSEQUAL',
    'DELETE',
    'ON',
    'UNION',
    'DENY',
    'OPEN',
    'UNIQUE',
    'DESC',
    'OPENDATASOURCE',
    'UNPIVOT',
    'DISK',
    'OPENQUERY',
    'UPDATE',
    'DISTINCT',
    'OPENROWSET',
    'UPDATETEXT',
    'DISTRIBUTED',
    'OPENXML',
    'USE',
    'DOUBLE',
    'OPTION',
    'USER',
    'DROP',
    'OR',
    'VALUES',
    'DUMP',
    'ORDER',
    'VARYING',
    'ELSE',
    'OUTER',
    'VIEW',
    'END',
    'OVER',
    'WAITFOR',
    'ERRLVL',
    'PERCENT',
    'WHEN',
    'ESCAPE',
    'PIVOT',
    'WHERE',
    'EXCEPT',
    'PLAN',
    'WHILE',
    'EXEC',
    'PRECISION',
    'WITH',
    'EXECUTE',
    'PRIMARY',
    'WITHIN GROUP',
    'EXISTS',
    'PRINT',
    'WRITETEXT',
    'EXIT',
    'PROC',
    ]

DATE_TY_SQL = '''
'''

DATE_LY_SQL = '''
SELECT ly_day_dt
FROM ABA_Views_SV.calendar_inf
WHERE day_dt = ?;'''

DIV_0 = '-' * 80

ENCODINGS = [
    # "utf8",
    # "ASCII",
    "windows-1252",
    "ISO-8859-1",
    # "SQL_Latin1_General_CP1_CI_AS",
    "latin_1",
    "big5",
    "big5hkscs",
    "cp037",
    "cp424",
    "cp437",
    "cp500",
    "cp737",
    "cp775",
    "cp850",
    "cp852",
    "cp855",
    "cp856",
    "cp857",
    "cp860",
    "cp861",
    "cp862",
    "cp863",
    "cp864",
    "cp865",
    "cp866",
    "cp869",
    "cp874",
    "cp875",
    "cp932",
    "cp949",
    "cp950",
    "cp1006",
    "cp1026",
    "cp1140",
    "cp1250",
    "cp1251",
    "cp1252",
    "cp1253",
    "cp1254",
    "cp1255",
    "cp1256",
    "cp1257",
    "cp1258",
    "euc_jp",
    "euc_jis_2004",
    "euc_jisx0213",
    "euc_kr",
    "gb2312",
    "gbk",
    "gb18030",
    "hz",
    "iso2022_jp",
    "iso2022_jp_1",
    "iso2022_jp_2",
    "iso2022_jp_2004",
    "iso2022_jp_3",
    "iso2022_jp_ext",
    "iso2022_kr",
    "iso8859_2",
    "iso8859_3",
    "iso8859_4",
    "iso8859_5",
    "iso8859_6",
    "iso8859_7",
    "iso8859_8",
    "iso8859_9",
    "iso8859_10",
    "iso8859_13",
    "iso8859_14",
    "iso8859_15",
    "johab",
    "koi8_r",
    "koi8_u",
    "mac_cyrillic",
    "mac_greek",
    "mac_iceland",
    "mac_latin2",
    "mac_roman",
    "mac_turkish",
    "ptcp154",
    "shift_jis",
    "shift_jis_2004",
    "shift_jisx0213",
    "utf_32",
    "utf_32_be",
    "utf_32_le",
    "utf_16",
    "utf_16_be",
    "utf_16_le",
    "utf_7",
    "utf_8_sig",
    ]