__author__  = 'drew bailey'
__version__ = 0.1

"""
The server object acts as a database server connection object. It is designed to facilitate SQL operations for all its
 Root children.
"""

from ...config import MASTER_PATH, MASTER_DB_NAME, MASTER_TABLES
from ...util import broadcast
from ..sql import (SQLiteBackends, ODBCBackends, TeradataBackends, IBMDB2Backends, MSSQLServerBackends, OracleBackends,
                   BACKENDS, MASTER_MEMORY, MASTER)
from .root import Root
from .table import Table
from .database import Database
import pyodbc
import sqlite3
import os


MASTER_STRINGS = ['master', ]
MASTER_MEMORY_STRINGS = ['master_memory', 'master memory', ]
LOCAL_STRINGS = ['local', ':local:', ]
MEMORY_STRINGS = ['memory', 'mem', ':memory:', 'temp', 'temporary', 'lite', 'sqlite', 'sqlite3', ]
PYODBC_STRINGS = ['external', 'odbc', 'pyodbc', ]
TERADATA_STRINGS = []
IBMDB2_STRINGS = []
MSSQLSERVER_STRINGS = []
ORACLE_STRINGS = ['oracle']

# WORKING_STRINGS = ['working', 'workspace', 'work']


class Server(Root):
    """

    """

    parent_classes = [None, Root]
    child_classes = [Database, Table]
    # default_child_name = 'default'
    connector_classes = []
    # (engine, connections_class, string_identifiers, connection_string)
    connection_key = {0: ('sqlite3', sqlite3, MASTER_MEMORY_STRINGS, ':memory:'),
                      1: ('sqlite3', sqlite3, MASTER_STRINGS, MASTER_PATH+'\\'+MASTER_DB_NAME),  # not used
                      2: ('sqlite3', sqlite3, MEMORY_STRINGS, ':memory:'),
                      3: ('sqlite3', sqlite3, LOCAL_STRINGS, None),
                      5: ('pyodbc', pyodbc, PYODBC_STRINGS, None),
                      6: ('oracle', pyodbc, ORACLE_STRINGS, None),  # TODO: change to oracle connection package.
                      }
    _text_factory = str
    detect_types = sqlite3.PARSE_COLNAMES

    def __init__(self, ctype=None, children=(), name='', parent=None, **kwargs):
        """

        """
        super(Server, self).__init__(children=children, name=name, parent=parent, **kwargs)
        self.con = kwargs.get('con', None)
        self.crs = kwargs.get('crs', None)
        self.con_str = kwargs.get('con_str', None)
        self.engine = kwargs.get('engine', None)  # package used to connect
        self.user_name = kwargs.get('user_name', '')
        self.path = kwargs.get('path', os.getcwd())

        if ctype:
            self.connect(ctype)

    # TODO: close connections?
    # def __del__(self):
    #     pass

    @property
    def c(self):
        return self.con, self.crs

    @c.setter
    def c(self, value):
        pass

    @c.deleter
    def c(self):
        self.crs.close()
        self.con.close()

    # TODO: Use backends object?
    @property
    def description(self):
        try:
            return self.crs.description
        except AttributeError:
            return

    @property
    def text_factory(self):
        return self._text_factory

    @text_factory.setter
    def text_factory(self, value):
        self._text_factory = value
        self.con.text_factory = value

    @text_factory.deleter
    def text_factory(self):
        self._text_factory = str
        self.con.text_factory = str

    def connect(self, ctype):
        # if isinstance(ctype, basestring):
        #     try:
        #         ctype = ctype.lower().strip()
        #     except AttributeError:
        #         pass

        if isinstance(ctype, int):
            self.__load_connection(ctype)
        elif ctype.lower() in MASTER_STRINGS:
            self.con, self.crs = self.c = MASTER
        elif ctype.lower() in MASTER_MEMORY_STRINGS:
            self.con, self.crs = self.c = MASTER_MEMORY
        elif ctype.lower() in MEMORY_STRINGS:
            self.__load_connection(2)
        elif ctype.lower() in LOCAL_STRINGS:
            self.__load_connection(3, LOCAL_STRINGS[0])
        elif ctype.lower() in PYODBC_STRINGS:
            self.__load_connection(5)
        elif ctype.lower() in ORACLE_STRINGS:
            self.__load_connection(6)
        elif ctype.lower().endswith('.db'):
            self.__load_connection(3, ctype)
        else:
            # defaults to trying an pyodbc connection with whatever is entered.
            self.__load_connection(5, ctype)

        if self.is_lite():
            self.engine = sqlite3
            if not self.backends:
                self.backends = SQLiteBackends()
        elif self.is_odbc():
            self.engine = pyodbc
            if not self.backends:
                self.backends = ODBCBackends()
        # elif self.is_oracle():
        #     self.engine = pyodbc
        #     if not self.backends:
        #         self.backends = OracleBackends()

    def __load_connection(self, index, other=None):
        source, engine, all_str, con_str = self.connection_key[index]
        self.engine = engine
        if not con_str:
            if self.con_str:
                con_str = self.con_str
            elif index < 5:
                con_str = self.__lite_con_str(other)
            else:
                con_str = self.__odbc_con_str(other)
        self.con = engine.connect(con_str)
        if self.is_lite():
            self.con.text_factory = self._text_factory
        self.crs = self.con.cursor()
        self.con_str = con_str

    def __lite_con_str(self, other):
        return self.__parse_path(other)

    def __odbc_con_str(self, other):
        return other.strip()

    def __parse_path(self, path):
        ext = '.db'
        # parse setup
        # try:
        base = os.path.basename(path)
        directory = os.path.dirname(path)

        if not directory:
            directory = self.path

        # except AttributeError:
        #     lst = path.split('\\')
        #     base = lst[-1]
        #     directory = '\\'.join(os.getcwd().split('\\')+lst[:-1])
        if directory.startswith('\\'):
            directory = self.path+directory
        # set extension
        if not base.lower().endswith(ext):
            base += ext
        # set name if not name
        if not self.name:
            self.name = base[:-len(ext)]
        # set / get path
        if directory:
            self.path = directory
        elif self.path:
            directory = self.path
        else:
            self.path = os.getcwd()
            directory = os.getcwd()
        # print 'directory:', directory
        # print 'base:', base
        return directory+'\\'+base

    def close(self):
        """ Commits and closes specified connection. """
        try:
            self.con.commit()
        except AttributeError, msg:
            broadcast(msg=msg, clamor=9)
            del self.con, self.crs
            self.con = None
            self.crs = None
            return
        if self.crs:
            try:
                self.crs.close()
            except AttributeError, msg:
                broadcast(msg=msg, clamor=9)
        try:
            self.con.close()
        except AttributeError, msg:
            broadcast(msg=msg, clamor=9)
        finally:
            del self.con, self.crs
            self.con = None
            self.crs = None

    def move_file(self, path):
        raise NotImplementedError()

    # only methods that modify a database should use db_ prefix.
    def get_table_names(self):
        return self.backends.get_table_names(connect=self.c)

    def db_pass_sql(self, sql, values=None):
        """

        :param sql:
        :param values:
        :return:
        """
        return self.backends.pass_sql(connect=self.c, sql=sql, values=values)

    def db_drop_all(self, protected=True):  # lite only, protects from horrible crushing drop all
        if self.is_lite():
            tables = self.get_table_names()
            for table in tables:
                if protected and table in MASTER_TABLES:
                    continue
                self.backends.drop_table(connect=self.c, table=table)
            self.db_vacuum()

    def _db_drop_protected(self):
        if self.is_lite():
            tables = self.get_table_names()
            for table in tables:
                if table in MASTER_TABLES:
                    self.backends.drop_table(connect=self.c, table=table)
            self.db_vacuum()

    def _db_write_protected(self):
        if self.is_lite():
            pass

    def db_vacuum(self):
        self.backends.vacuum(connect=self.c)

    def db_combine(self, other):
        """
        Adds all database features from other to self if not already.
        :param other:
        :return:
        """
        pass

    def db_attach(self):
        pass

    def is_lite(self):
        if isinstance(self.con, sqlite3.Connection):
            return True
        return False

    def is_odbc(self):
        if isinstance(self.con, pyodbc.Connection):
            return True
        return False

    def select(self, table_name, schema=None, fields=None, where=None, no_case=False, name='', **kwargs):
        """

        Makes a table from a select on a single database table. Essentially a subset.
        :param table_name: Table name.
        :param schema:
        :param fields: List of field names to select.
        :param where: list of tuples containing (field, value) pairs, where field = value. [(field, value),]
        :param no_case: A parameter that removes case comparison from SQLite queries, does not work on most ODBC
            connections.
        :param name:
        :param kwargs:
        :return:
        """
        self.append(Table.from_select(
            self.c,
            table_name=table_name,
            schema=schema,
            fields=fields,
            where=where,
            no_case=no_case,
            name=name,
            parent=self,
            kwargs=kwargs
        ))

    # TODO: implement this.
    def db_drop_all_before(self):
        # drop entries where run date is 6 mo. old? or given field is? rightmost date field?
        pass

    # TODO: test this.
    def db_drop_before(self, table_name, field_name, date):
        sql = 'DELETE FROM %s WHERE %s < ?;' % (table_name, field_name)
        self.backends.pass_sql(connect=self.c, sql=sql, values=[date])
