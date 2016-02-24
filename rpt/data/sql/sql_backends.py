__author__  = 'drew bailey'
__version__ = 3.1

"""
Functions using pyodbc and sqlite3.
No rpt prerequisites.
Used by many internal methods.

REBUILD:
    - root class with common methods between database engines.
    - a child class for each supported database engine.
    - a master class that calls whichever child is necessary for the backend in use.

docs need a lot of work.
"""

import re

from ...config import MASTER_TABLES, USER_TABLE, CONNECTION_TABLE, CRON_LOG_TABLE, LOG_TABLE, GLOBAL_TABLE
from ...util import clean, broadcast
from sqlite3 import PARSE_COLNAMES, InterfaceError as sqlite3InterfaceError
from pyodbc import InterfaceError as pyodbcInterfaceError


NOT_IMPLEMENTED_TEXT = 'Method not yet implemented for database engine class %s.'


class SQLBackends(object):
    """
    Handles SQLite3 and pyodbc connection types.
    Central Concepts:
        connect -> a tuple containing a connection object and a cursor object ex => connect=(con, crs)
        # ctype -> generates or refers to connections from keyword, login credentials, sqlite file name (.db), or full
        #     connection string.
        # defaults -> from user_settings table
        # connections -> from connection_settings table
    """

    memory_strings = ['mem', 'memory', ':memory:', 'temp', 'temporary']
    local_strings = ['local', ':local:', '']
    external_strings = ['odbc', 'external', 'e_db']

    text_factory = str
    detect_types = PARSE_COLNAMES

    clean_level = 1
    explicit_naming = True  # create table is always explicit on fields

    def __init__(self):
        pass

    def __str__(self):
        return "SQLBackends engine object at %s." % id(self)

    def __repr__(self):
        return self.__str__()

    def _not_implemented(self):
        raise NotImplementedError(NOT_IMPLEMENTED_TEXT % self.__class__)

    def _process_table(self, table, schema=None):
        table = clean(raw=table, sql=self.clean_level)
        table = table.replace(' ', '_')
        if schema:
            schema = clean(raw=schema, sql=self.clean_level)
            schema = schema.replace(' ', '_')
            if self.explicit_naming:
                table = '"%s"."%s"' % (schema, table)
            else:
                table = '%s.%s' % (schema, table)
        else:
            if self.explicit_naming:
                table = '"%s"' % table
            else:
                table = '%s' % table
        return table

    # ## INFORMATION QUERIES ###
    @staticmethod
    def description(connect):
        """
        Returns cursor.description.
        [(name, type_code, display_size, internal_size, precision, scale, null_ok),]
        :param connect:
        :return: [(name, type_code, display_size, internal_size, precision, scale, null_ok),]
        """
        con, crs = connect
        return crs.description

    @staticmethod
    def header(connect):
        """
        Returns a header list from a connection's last execute.
        :param connect:
        :return:
        """
        con, crs = connect
        return [x[0] for x in crs.description]

    @staticmethod
    def types(connect):
        """
        Returns a field types list from a connection's last execute.

        :param connect:
        :return: [type1, type2, ]
        """
        con, crs = connect
        return [x[1] for x in crs.description]

    def version(self, connect):
        self._not_implemented()

    def get_databases(self, connect):
        self._not_implemented()

    def get_database_names(self, connect, table_type=None):
        rtrn = self.get_tables(connect=connect, table_type=table_type)
        return list(set([x[1] for x in rtrn]))

    def get_tables(self, connect, schema=None, table_type=None):
        self._not_implemented()

    def get_table_names(self, connect, schema=None, table_type=None):
        rtrn = self.get_tables(connect=connect, schema=schema, table_type=table_type)
        return list(set([x[2] for x in rtrn]))

    def get_table(self, connect, table, schema=None, table_type=None):
        self._not_implemented()

    def get_indices(self, connect, table=None, schema=None):
        self._not_implemented()

    def get_indexed_fields(self, connect, table, schema=None):
        rtrn1 = self.get_keys(connect=connect, table=table, schema=schema, primary=True)
        rtrn2 = self.get_keys(connect=connect, table=table, schema=schema, primary=False)
        return list(set([x[3] for x in rtrn1+rtrn2]))

    def get_keys(self, connect, table, schema=None, primary=True):
        self._not_implemented()

    def get_row_id(self, connect, schema, table):
        self._not_implemented()

    def get_columns(self, connect, schema, table):
        self._not_implemented()

    def get_column(self, connect, schema, table, column):
        self._not_implemented()

    # ## TABLE SELECTION ###

    def select(self, connect, table, schema=None, fields=None, where=None, nocase_compare=False):
        """
        Selects table data.
        :param connect:
        :param table: 'table_name'
        :param fields: [field_1, field_2, ..., field_n, ]
        :param where: [(field_a, value_a), ..., (field_x, value_x), ]
        :param nocase_compare:
        :return:
        """
        con, crs = connect
        sql, vals = self.generate_select_sql(table=table, schema=schema, fields=fields, where=where, nocase_compare=nocase_compare)
        broadcast(msg=sql, clamor=7)
        # print sql
        if vals:
            rtrn = crs.execute(sql, vals).fetchall()
        else:
            rtrn = crs.execute(sql).fetchall()
        return rtrn

    def generate_select_sql(self, table, schema, fields, where, nocase_compare):
        """

        :param table:
        :param schema:
        :param fields:
        :param where:
        :param nocase_compare:
        :return:
        """
        table = self._process_table(table=table, schema=schema)
        sql = '''SELECT '''
        if fields:
            for field in fields:
                sql += '\n '+clean(raw=field, sql=self.clean_level)+','
            sql = sql[:-1]
        else:
            sql += '*'
        sql += '\nFROM '+table
        if where:
            flds, vals = zip(*where)
            sql += '\nWHERE %s = ? ' % ' = ?\n AND '.join([clean(raw=fld, sql=self.clean_level) for fld in flds])+' \n'
            if nocase_compare:
                sql += ' COLLATE NOCASE\n'
            sql += ';'
        else:
            vals = None
        return sql, vals

    @staticmethod
    def pass_sql(connect, sql, values=None):
        """
        Passes sql code to connection.
        :param connect:
        :param sql: Full sql code to pass to database.
        :param values:
        :return:
        """
        con, crs = connect
        if values:
            rtrn = crs.execute(sql, values).fetchall()
        else:
            broadcast(msg=sql, clamor=7)
            # print sql
            rtrn = crs.execute(sql).fetchall()
        if not rtrn:
            con.commit()
        return rtrn

    def sample(self, connect, table, database=None, n=100):
        """

        :param connect:
        :param table:
        :param database:
        :param n:
        :return:
        """
        rtrn = self.__sample(connect=connect, database=database, table=table, n=n)
        if rtrn:
            return rtrn

    def __sample(self, connect, database, table, n):
        self._not_implemented()

    # ## TABLE MANAGEMENT ###

    # # CREATE ##
    def create_table(self, connect, table, fields, schema=None, data_types=None, if_not=True):
        """

        :param connect:
        :param table:
        :param fields:
        :param schema:
        :param data_types:
        :param if_not:
        :return:
        """
        con, crs = connect
        table = self._process_table(table=table, schema=schema)

        fields = ', '.join(self._generate_write_fields(fields=fields, data_types=data_types))
        if if_not:
            sql = """CREATE TABLE IF NOT EXISTS %s(%s);"""
        else:
            sql = """CREATE TABLE %s(%s);"""
        # print 'from SQLBackends.create_table'
        # print sql % (table, fields)
        crs.execute(sql % (table, fields))
        con.commit()
        broadcast(msg='''**TABLE CREATED**\nTable: %s\nFields: %s''' % (table, fields), clamor=7)

    def create_index(self, connect, index, table, fields, unique=False):
        con, crs = connect
        table = clean(raw=table, sql=self.clean_level)
        table = table.replace(' ', '_')
        index = clean(raw=index, sql=self.clean_level)
        sql_unique = 'CREATE UNIQUE INDEX %s ON %s (%s);' % \
                     (index, table, ','.join(['"'+str(x)+'"' for x in fields]))
        sql = 'CREATE INDEX %s ON %s (%s);' % (index, table, ','.join(['"'+str(x)+'"' for x in fields]))
        if unique:
            crs.execute(sql_unique)
        else:
            crs.execute(sql)
        con.commit()

    # # DESTROY ##
    # TODO: Make these methods safe. Only drop from lite connections?

    def drop_table(self, connect, table):
        # make safe
        con, crs = connect
        table = clean(raw=table, sql=self.clean_level)
        table = table.replace(' ', '_')
        sql = "DROP TABLE %s;" % table
        crs.execute(sql)
        con.commit()
        broadcast(msg='Table %s dropped, SQL: %s.' % (table, sql), clamor=9)

    def drop_tables(self, connect, preserve=True):
        # NOT CURRENTLY USED
        """

        :param connect:
        :param preserve: Preserves master tables.
        :return:
        """
        # tables = self.get_table_names(connect=connect)
        tables = []
        for table in tables:
            if preserve and table.lower() in MASTER_TABLES:
                continue
            self.drop_table(connect=connect, table=table)

    @staticmethod
    def drop_index(connect, index_name):
        # make safe
        con, crs = connect
        crs.execute('''DROP INDEX %s;''' % index_name)
        con.commit()
        # broadcast(msg='Index %s dropped from %s.' % (index, ctype), clamor=9)

    def drop_indices(self, connect, preserve=True):
        # make safe
        """
        Removes all indices in database.
        :param connect:
        :param preserve: Preserves indices on master tables.
        :return:
        """
        indices = self.get_indices(connect=connect)
        for index in indices:
            if preserve and index[2].lower() in MASTER_TABLES:
                continue
            self.drop_index(connect=connect, index_name=index[1])
        broadcast(msg='All indices dropped from %s.' % connect, clamor=9)

    def drop_rows(self, connect, table, where):
        # make safe
        """
        Drops rows where conditions are met.
        :param connect:
        :param table:
        :param where: [(field_a, value_a), ..., (field_x, value_x), ]
        :return:
        """
        con, crs = connect
        table = clean(raw=table, sql=self.clean_level)
        flds, vals = zip(*where)
        flds = ' = ? AND '.join(self._generate_fields(fields=flds))
        sql = 'DELETE FROM %s WHERE %s = ?' % (table, flds)
        # print sql
        # print vals
        crs.execute(sql, vals)
        con.commit()
        broadcast(msg='Rows dropped from %s, SQL: %s' % (table, sql), clamor=9)

    # TODO: implement.
    def drop_field(self, connect, table, field):
        self._not_implemented()

    # # ALTER ##

    def insert_field(self, connect, table, field, data_type=None, default=None):
        con, crs = connect
        table = self._process_table(table=table)
        field = self._generate_write_field(field=field, data_type=data_type)
        sql = "ALTER TABLE %s ADD %s DEFAULT %s;" % (table, field, default)
        crs.execute(sql)
        con.commit()
        broadcast(msg='Field added to %s, SQL: %s' % (table, sql), clamor=9)

    def insert_rows(self, connect, table, rows, fields=None, schema=None, exceptions=()):
        con, crs = connect
        table = self._process_table(table=table, schema=schema)

        values_sql = ','.join(['?' for x in range(len(rows[0]))])
        if fields:
            sql = "INSERT INTO %s (%s) VALUES(%s);" % \
                  (table, ', '.join(self._generate_fields(fields=fields)), values_sql)
        else:
            sql = "INSERT INTO %s VALUES(%s);" % (table, values_sql)

        # TODO: better way of allowing pytypes in tables but converting on write? check columns that don't type well?
        e = (sqlite3InterfaceError, pyodbcInterfaceError) + exceptions
        try:
            # print sql
            # print rows
            crs.executemany(sql, rows)
        except e:
            for row in rows:
                try:
                    crs.execute(sql, row)
                except e:
                    new_row = []

                    for element in row:
                        if not isinstance(element, basestring) and hasattr(element, '__iter__'):
                            new_row.append(str(element))
                            # print str(element)
                        else:
                            new_row.append(element)
                    crs.execute(sql, new_row)
        con.commit()
        broadcast(msg='**Rows inserted into %s**\n SQL:\n%s' % (table, sql), clamor=9)

    def alter_values(self, connect, table, where, sets=None, schema=None):
        # make safe
        con, crs = connect
        table = self._process_table(table=table, schema=schema)
        if not sets:
            sets = where  # TODO: so... it does nothing if sets = where
        set_fields, set_values = zip(*sets)
        check_fields, check_values = zip(*where)
        sets_sql = ' = ? AND '.join(self._generate_fields(fields=set_fields))
        checks_sql = ' = ? AND '.join(self._generate_fields(fields=check_fields))
        sql = 'UPDATE %s SET %s = ? WHERE %s = ?;' % (table, sets_sql, checks_sql)
        crs.execute(sql, tuple(list(set_values)+list(check_values)))
        con.commit()
        broadcast(msg='Values updated in table %s, SQL: %s' % (table, sql), clamor=9)  # TODO: not correct log message.
        # self.reindex(connect=connect, table=table)

    def reindex(self, connect, table, schema=None):
        con, crs = connect
        table = self._process_table(table=table, schema=schema)
        sql = 'REINDEX %s;' % table
        crs.execute(sql)
        con.commit()

    def rename_table(self, connect, old, new):
        # make safe
        con, crs = connect
        old = self._process_table(table=old)
        new = self._process_table(table=new)
        sql = 'ALTER TABLE %s RENAME TO %s' % (old, new)
        crs.execute(sql)
        con.commit()
    # no... must be same db
    # def insert_table_to_table(self, from_table, into_table, from_ctype='memory', into_ctype='memory'):
    #     into_con, into_crs = self._connection_from_ctype(ctype=into_ctype)
    #     from_con, from_crs = self._connection_from_ctype(ctype=from_ctype)
    #     into_table = clean(raw=into_table, sql=self.clean_level)

    # TODO: implement.
    def rename_field(self, connect, table, old, new):
        # use UPDATE on sqlite_master table? Can corrupt entire database if there is a syntax error...
        self._not_implemented()

    # ## SQL GENERATION ###
    def generate_join_sql(self, table1, table2, how, on, database=None):
        self._not_implemented()

    @staticmethod
    def _generate_select(*args):
        """
        For now generates select all from table names in order of entry.
        :param *args: takes any number of table names.
        :return: SELECT t1.*, t2.*, ..., tn.*
        """
        sql = '\nSELECT '
        for arg in args:
            sql += '%s.*, ' % arg
        sql = sql[:-2]+' '
        return sql

    def _generate_on(self, table1, table2, on):
        """
        Generates on statement for given tables and on entry.
        on = [(t1.col1, t2.col1), (t1.col2, t2.col2),...]
        if droptag returns list of redundant column names to drop.
        """
        starter = '\nON '
        joiner = '\n AND '
        ons = []
        for fields in on:
            c1, c2 = self._generate_write_fields(fields=fields)
            ons.append(table1+'.'+c1+' = '+table2+'.'+c2+' ')
        sql = starter+joiner.join(ons)
        return sql

    def _generate_where(self, table, fields, values):
        """
        Generates WHERE SQL code.
        :return:
        """
        starter = '\nWHERE '
        joiner = '\n AND '
        wheres = []
        fields = self._generate_write_fields(fields=fields)
        for f, v in zip(fields, values):
            if isinstance(v, basestring):
                if str(v).lower().strip() in['null', 'not null']:
                    wheres.append("%s.%s IS %s " % (table, f, v))
                else:
                    wheres.append("%s.%s = '%s' " % (table, f, v))
            else:
                wheres.append("%s.%s = %s " % (table, f, v))
        sql = starter+joiner.join(wheres)
        return sql

    def _generate_group(self):
        pass

    def _generate_order(self):
        pass

    def _generate_write_fields(self, fields, data_types=None):
        new_fields = []
        if data_types:
            for field, data_type in zip(fields, data_types):
                new_fields.append(self._generate_write_field(field=field, data_type=data_type))
        else:
            for field in fields:
                new_fields.append(self._generate_write_field(field))
        return new_fields

    def _generate_write_field(self, field, data_type=None):
        field = clean(raw=field, sql=self.clean_level)
        if data_type:
            entry = data_type.split()
            return '"'+str(field)+'" ['+str(entry[0])+']'+' '+' '.join(entry[1:])
        return '"'+str(field)+'"'

    def _generate_fields(self, fields):
        new_fields = []
        for field in fields:
            field = clean(raw=field, sql=self.clean_level)
            if self.explicit_naming:
                new_fields.append('"'+str(field)+'"')
            else:
                new_fields.append(str(field))
        return new_fields
    # def attach(self, connect, other):
    #     sql = "ATTACH DATABASE 'NEW.db' AS 'NEW'"
    #
    # def detach(self, connect):

    @staticmethod
    def pretty_format(sql):
        sql = sql.replace(",", ",\n")
        sql = sql.replace("(", "(\n")
        sql = sql.replace(")", "\n)")
        return sql


class SQLiteBackends(SQLBackends):

    def __init__(self):
        super(SQLiteBackends, self).__init__()

    # SQLite only methods #
    @staticmethod
    def vacuum(connect):
        """
        Cleans a SQLite database associated with default local connection, removes empty space and file fragments.
        :return: None
        """
        con, crs = connect
        con.execute('vacuum')
        con.commit()

    @staticmethod
    def get_logs(connect, n=10):
        """
        Package primary logs.
        :param connect:
        :param n:
        :return:
        """
        con, crs = connect
        rtrn = crs.execute('SELECT * FROM %s ORDER BY CREATED DESC LIMIT %i' % (LOG_TABLE, n)).fetchall()
        return rtrn

    @staticmethod
    def get_run_logs(connect, n=10):
        """
        Cron run logs.
        :param connect:
        :param n:
        :return: n most recent log entries in the Cron log table.
        """
        con, crs = connect
        rtrn = crs.execute('SELECT * FROM %s ORDER BY CREATED DESC LIMIT %i' % (CRON_LOG_TABLE, n)).fetchall()
        return rtrn

    @staticmethod
    def get_global_defaults(connect):
        """

        :param connect:
        :return:
        """
        con, crs = connect
        rtrn = crs.execute('''
        SELECT *
        FROM %s
        ;''' % GLOBAL_TABLE).fetchall()
        return rtrn

    @staticmethod
    def get_user_defaults(connect, user_name):
        """
        Returns locally loaded user default fields.
        :param user_name:
        :return: (DEFAULT_DATABASE, DEFAULT_CONNECTION_STRING, DEFAULT_EMAIL)
        """
        con, crs = connect
        rtrn = crs.execute('''
        SELECT
            DEFAULT_DATABASE,
            DEFAULT_CONNECTION_STRING,
            DEFAULT_EMAIL
        FROM %s
        WHERE USER_NAME = ?
        ;''' % USER_TABLE, (user_name,)).fetchall()
        return rtrn

    @staticmethod
    def get_user_connections(connect, user_name):
        """

        :param user_name:
        :return: [(DATABASE, CONNECTION_STRING), ]
        """
        con, crs = connect
        rtrn = crs.execute('''SELECT DATABASE, CONNECTION_STRING FROM %s WHERE USER_NAME = ?;'''
                           % CONNECTION_TABLE, (user_name,)).fetchall()
        return rtrn

    @staticmethod
    def get_user_connection(connect, user_name, database):
        """

        :param user_name:
        :param database:
        :return: CONNECTION_STRING
        """
        con, crs = connect
        rtrn = crs.execute('''
        SELECT
            DATABASE,
            CONNECTION_STRING
        FROM %s
        WHERE USER_NAME = ?
        AND DATABASE = ?
        ;''' % CONNECTION_TABLE, (user_name, database,)).fetchall()
        return rtrn
    # SQLite specific code overwriting root methods #

    def get_databases(self, connect):
        """

        :param connect:
        :return:
        """
        con, crs = connect
        rtrn = crs.execute('PARGMA database_list').fetchall()
        return rtrn

    def get_tables(self, connect, schema=None, table_type=None):
        """
        Returns all table information for specified connection.
        :param connect:
        :return: [('type', 'name', 'tbl_name', 'rootpage', 'sql'), ]
        """
        con, crs = connect
        rtrn = crs.execute("SELECT * FROM SQLITE_MASTER WHERE type='table';").fetchall()
        return rtrn

    def get_table(self, connect, table, schema=None, table_type=None):
        """
        Returns all table information for specified connection.
        :param connect:
        :return: [('type', 'name', 'tbl_name', 'rootpage', 'sql'), ]
        """
        con, crs = connect
        rtrn = crs.execute("SELECT * FROM SQLITE_MASTER WHERE type='table' AND name=? COLLATE NOCASE;",
                           (table,)).fetchall()
        return rtrn

    def get_columns(self, connect, schema, table):
        """
        PRAGMA table_info(table_name);
            results in: [('cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'),]
            Name is index [1], Type is index [2] for each row.
        :param connect: (connect, cursor) for a lite source
        :param table: table name
        :return: table info as fetchall [(),]
        """
        con, crs = connect
        rslt = crs.execute('PRAGMA table_info(%s);' % table).fetchall()
        rtrn = [(table, row[1], row[2]) for row in rslt]
        return rtrn

    def get_column(self, connect, schema, table, column):
        con, crs = connect
        rslt = crs.execute('PRAGMA table_info(%s);' % table).fetchall()
        for row in rslt:
            if row[1].lower() == column.lower():
                return row

    # tested
    def get_indices(self, connect, table=None, schema=None):
        """
        Returns index information for specified sqlite connection, for specified table or all tables.
        :param connect: (sqlite3.connect('database'), sqlite3.connect('database').cursor())
        :param table:
        :return: [('table_name', 'index_name', [list, of, fields,], unique_bool),]
        """
        con, crs = connect
        if table:
            rslt = crs.execute("PRAGMA index_list(%s);" % table).fetchall()
            rtrn = []
            for sql, index_name, unique_value in rslt:
                unique = False
                if unique_value:
                    unique = True
                fields = [row[2] for row in self.__get_index_info(connect=connect, index=index_name)]
                rtrn.append((table, index_name, fields, unique))
        else:
            rslt = crs.execute("SELECT * FROM SQLITE_MASTER WHERE type='index';").fetchall()
            rtrn = []
            for object_type, index_name, table_name, rootpage, sql in rslt:
                unique = False
                if 'unique' in sql.lower():
                    unique = True
                fields = [row[2] for row in self.__get_index_info(connect=connect, index=index_name)]
                rtrn.append((table_name, index_name, fields, unique))
        return rtrn

    @staticmethod
    def __get_index_info(connect, index):
        """
        Returns index information for specified connection and index name.
        :param connect:
        :return: ['seqno', 'cid', 'name']
        """
        con, crs = connect
        # rtrn = crs.execute("SELECT * FROM SQLITE_MASTER WHERE type='index' AND name=?;", (index,)).fetchall()
        rtrn = crs.execute("PRAGMA index_info(%s);" % index).fetchall()
        return rtrn

    def version(self, connect):
        """
        :param connect:
        :return: SQLite3 version string.
        """
        con, crs = connect
        return "SQLite Version: %s" % crs.execute('SELECT SQLITE_VERSION()').fetchone()

    def __sample(self, connect, database, table, n):
        """

        :param connect:
        :param database:
        :param table:
        :param n:
        :return:
        """
        con, crs = connect
        sql = 'select * from %s limit %i;' % (table, n)
        rtrn = crs.execute(sql).fetchall()
        return rtrn

    def generate_join_sql(self, table1, table2, how, on, database=None):
        how = how.lower().strip()
        outer, right = False, False
        if how == 'outer':
            outer = True
            how = 'left'
        elif how == 'right':
            # right = True
            table1, table2 = table2, table1
            how = 'left'
        s = self._generate_select(table1, table2)
        j = self._generate_join(how=how)
        o = self._generate_on(table1=table1, table2=table2, on=on)

        fields = [c1 for c1, c2 in on]
        if outer:  # sqlite outer join replacement, non-native join type.
            w = self._generate_where(table=table1, fields=fields, values=['NULL']*len(fields))
            sql = """
%(select)s
FROM %(table1)s
%(join)s %(table2)s
%(on)s
UNION ALL
%(select)s
FROM %(table2)s
%(join)s %(table1)s
%(on)s
%(where)s;
""" % {'select': s, 'join': j, 'on': o, 'where': w, 'table1': table1, 'table2': table2}
        else:
            sql = """
%(select)s
FROM %(table1)s
%(join)s %(table2)s
%(on)s;
""" % {'select': s, 'join': j, 'on': o, 'table1': table1, 'table2': table2}
        return sql

    @staticmethod
    def _generate_join(how):
        """ Generates join type from keywords. Expected 'inner', 'outer', 'left', or 'right'."""
        how = how.lower()
        if how == 'inner':
            join = "\nJOIN "
        elif how == 'left':
            join = '\nLEFT JOIN '
        elif how == 'right':
            join = '\nRIGHT JOIN '
            broadcast(msg='Right join executed as a flipped left join for sqlite3 connections. How did you get here?',
                      clamor=7)
            # raise NotImplementedError('Right join executed as a flipped left join for sqlite3 connections. '
            #                           'How did you get here?')
        elif how == 'union':
            join = '\nUNION '
            broadcast(msg='Unions are more easily done through python methods and are not currently supported.',
                      clamor=7)
            # raise NotImplementedError('Unions are more easily done through python methods and are not currently '
            #                           'supported.')
        else:
            # join = '\nJOIN '
            raise ValueError('Join type not recognized.')
        return join

    def __rename_field(self, connect, table, old, new):
        # use drop_field code for a good starting place
        con, crs = connect
        fields = get_fields_somehow
        data_types = get_types_somehow
        fields = ', '.join(self._generate_write_fields(fields=fields, data_types=data_types))
        replace = {'table': table,
                   'old': clean(raw=old, sql=self.clean_level),
                   'new': clean(raw=new, sql=self.clean_level),
                   'fields': fields,
                   }
        sql0 = 'ALTER TABLE %(table)s RENAME TO %(table)s_temp;' % replace
        sql1 = 'CREATE TABLE %(table)s (%(fields)s);' % replace
        sql2 = 'INSERT INTO %(table)s (%(fields)s) SELECT %(fields)s from %(table)s_temp' % replace
        sql3 = '' % replace  # change indices back to new table
        sql4 = 'DROP TABLE %(table)s_temp' % replace
        # execute all sql statements

    def drop_field(self, connect, table, field):
        con, crs = connect
        table = clean(raw=table, sql=self.clean_level)
        table = table.replace(' ', '_')
        temp = '%s_temp' % table
        field = clean(raw=field, sql=self.clean_level)
        fields = [fld for fld in self.get_columns(connect=connect, schema=None, table=table) if fld != field]
        sql = self.pretty_format(self.get_table(connect=connect, table=table)[0][0])
        lines = sql.splitlines()
        find_col = r'\b%s\b' % field
        keep_lines = [line for line in lines if not re.search(find_col, line)]
        create = re.sub(r',(\s*\))', r'\1', '\n'.join(keep_lines))
        self.rename_table(connect=connect, old=table, new=temp)

        crs.execute(create)
        # commit?
        replace = {'table': table,
                   'fields': fields,
                   }
        crs.execute('INSERT INTO %(table_temp)s ( %(fields)s ) SELECT %(fields)s FROM %(table)s);' % replace)
        # commit?
        # reassign indices
        # commit?
        self.drop_table(connect=connect, table=table+'_temp')
        con.commit()

    def alter_values(self, connect, table, where, sets=None, schema=None):
        super(SQLiteBackends, self).alter_values(connect=connect, table=table, where=where, sets=sets, schema=schema)
        self.reindex(connect=connect, table=table)

class ODBCBackends(SQLBackends):
    def __init__(self):
        super(ODBCBackends, self).__init__()

    def _generate_write_field(self, field, data_type=None):
        field = clean(raw=field, sql=self.clean_level)
        if data_type:
            entry = data_type.split()
            return '"'+str(field)+'" '+str(entry[0])+''+' '+' '.join(entry[1:])
        return '"'+str(field)+'"'

    # tested
    def get_tables(self, connect, schema=None, table_type=None):
        if schema and table_type:
            return connect[1].tables(schema=schema, tableType=table_type).fetchall()
        elif schema:
            return connect[1].tables(schema=schema).fetchall()
        elif table_type:
            return connect[1].tables(tableType=table_type).fetchall()
        else:
            return connect[1].tables().fetchall()

    # tested
    def get_table(self, connect, table, schema=None, table_type=None):
        if schema and table_type:
            return connect[1].tables(schema=schema, table=table, tableType=table_type).fetchone()
        elif schema:
            return connect[1].tables(schema=schema, table=table).fetchone()
        elif table_type:
            return connect[1].tables(table=table, tableType=table_type).fetchone()
        else:
            return connect[1].tables(table=table).fetchone()

    def get_columns(self, connect, schema, table):
        """

        pyodbc.Connect().Cursor().columns(...) = (
            able_cat
            table_schem
            table_name
            column_name
            data_type
            type_name
            column_size
            buffer_length
            decimal_digits
            num_prec_radix
            nullable
            remarks
            column_def
            sql_data_type
            sql_datetime_sub
            char_octet_length
            ordinal_position
            is_nullable: One of SQL_NULLABLE, SQL_NO_NULLS, SQL_NULLS_UNKNOWN.
            )

        :param connect:
        :param schema:
        :param table:
        :return: [table_name, column_name, data_type, ]
        """
        con, crs = connect
        rslt = crs.columns(table=table, schema=schema).fetchall()
        rtrn = [(row[2], row[3], row[5]) for row in rslt]
        return rtrn

    def get_column(self, connect, schema, table, column):
        """

        pyodbc.Connect().Cursor().columns(...) = (
            able_cat
            table_schem
            table_name
            column_name
            data_type
            type_name
            column_size
            buffer_length
            decimal_digits
            num_prec_radix
            nullable
            remarks
            column_def
            sql_data_type
            sql_datetime_sub
            char_octet_length
            ordinal_position
            is_nullable: One of SQL_NULLABLE, SQL_NO_NULLS, SQL_NULLS_UNKNOWN.
            )

        :param connect: (connect, cursor)
        :param schema: database name
        :param table: table name
        :param column: column name
        :return: if column in colmn_names returns fetchone() of that row, else None
        """
        con, crs = connect
        rslt = crs.columns(table=table, schema=schema).fetchall()
        for row in rslt:
            if row[1].lower() == column.lower():
                return row[2], row[3], row[5]

    def get_row_id(self, connect, schema, table):
        """
        This is really a best guess.
        :param connect:
        :param schema:
        :param table:
        :return:
        """
        con, crs = connect
        rtrn = crs.rowIdColumns(table=table, schema=schema).fetchall()
        return rtrn

    # TODO: SQLStatistics errors with "Error" when called on IBM DB2.
    def get_indices(self, connect, table=None, schema=None):
        """
        Returns index information for specified odbc connection, for specified table or all tables.
        pyodbc.connect('database').cursor().statistics(table, [schema]) =
            [(table_cat, table_schem, table_name, non_unique, index_qualifier, index_name, type, ordinal_position,
            column_name, asc_or_desc, cardinality, pages, filter_condition),]

        :param connect:
        :param table:
        :param schema:
        :return: [('table_name', 'index_name', [list, of, fields,], unique_bool),]
        """
        rslt = connect[1].statistics(table=table, schema=schema).fetchall()
        indices = list(set([(x[2], x[5], x[3]) for x in rslt if x[5]]))
        rtrn = []
        for table_name, index_name, non_unique in indices:
            fields = []
            for row in rslt:
                row_index_name = row[5]
                row_table_name = row[2]
                column_name = row[8]
                if index_name == row_index_name and table_name == row_table_name:
                    fields.append(column_name)
            unique = True
            if non_unique:
                unique = False
            rtrn.append((table_name, index_name, fields, unique))
        return rtrn

    def get_keys(self, connect, table, schema=None, primary=True):
        """

        :param connect:
        :param table:
        :param schema:
        :param primary:
        :return:
        """
        if schema and primary:
            rtrn = connect[1].primaryKeys(table=table, schema=schema).fetchall()
        elif schema:
            rtrn = connect[1].foreignKeys(table=table, schema=schema).fetchall()
        elif primary:
            rtrn = connect[1].primaryKeys(table=table).fetchall()
        else:
            rtrn = connect[1].foreignKeys(table=table).fetchall()
        return rtrn

    def generate_join_sql(self, table1, table2, how, on, database=None):
        how = how.lower().strip()
        s = self._generate_select(table1, table2)
        j = self._generate_join(how=how)
        o = self._generate_on(table1=table1, table2=table2, on=on)
        if database:
            sql = """
%(select)s
FROM %(database)s.%(table1)s
%(join)s %(database)s.%(table2)s
%(on)s;
""" % {'select': s, 'join': j, 'on': o, 'table1': table1, 'table2': table2, 'database': database}
        else:
            sql = """
%(select)s
FROM %(table1)s
%(join)s %(table2)s
%(on)s;
""" % {'select': s, 'join': j, 'on': o, 'table1': table1, 'table2': table2}
        return sql

    @staticmethod
    def _generate_join(how):
        """ Generates join type from keywords. Expected 'inner', 'outer', 'left', or 'right'."""
        how = how.lower()
        if how == 'inner':
            join = "\nJOIN "
        elif how == 'left':
            join = '\nLEFT JOIN '
        elif how == 'right':
            join = '\nRIGHT JOIN '
        elif how == 'union':
            join = '\nUNION '
        elif how == 'outer':
            join = '\nFULL OUTER JOIN '
        else:
            join = '\nJOIN '
            print 'Join type not recognized, defaulted to inner join.'
        return join


class TeradataBackends(ODBCBackends):
    def __init__(self):
        super(TeradataBackends, self).__init__()

    def __sample(self, connect, database, table, n):
        """

        :param connect:
        :param database:
        :param table:
        :param n:
        :return:
        """
        con, crs = connect
        if database:
            table = database+'.'+table
        sql = 'SELECT TOP %i * FROM %s;' % (n, table)
        rtrn = crs.execute(sql).fetchall()
        return rtrn


class IBMDB2Backends(ODBCBackends):
    def __init__(self):
        super(IBMDB2Backends, self).__init__()

    def __sample(self, connect, database, table, n):
        """

        :param connect:
        :param database:
        :param table:
        :param n:
        :return:
        """
        con, crs = connect
        if database:
            table = database+'.'+table
        sql = 'SELECT * FROM %s FETCH FIRST %i ROWS ONLY;' % (table, n)
        rtrn = crs.execute(sql).fetchall()
        return rtrn


class MSSQLServerBackends(ODBCBackends):
    def __init__(self):
        super(MSSQLServerBackends, self).__init__()


class OracleBackends(ODBCBackends):
    def __init__(self):
        super(OracleBackends, self).__init__()

    def create_table(self, connect, table, fields, schema=None, data_types=None, if_not=True):
        try:
            super(OracleBackends, self).create_table(
                connect=connect,
                table=table,
                fields=fields,
                schema=schema,
                data_types=data_types,
                if_not=False)
        except Exception, e:
            if if_not and e.args == ('HY000', '[HY000] [Oracle][ODBC][Ora]ORA-00955: name is already used by an existing object\n (955) (SQLExecDirectW)'):
                pass
            else:
                raise e
    def _generate_fields(self, fields):
        new_fields = []
        for field in fields:
            field = clean(raw=field, sql=self.clean_level)
            if field.lower() == 'rownum':
                new_fields.append(str(field))
            else:
                new_fields.append('"'+str(field)+'"')
        return new_fields
