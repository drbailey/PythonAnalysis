__author__  = 'drew bailey'
__version__ = 2.0


"""
User facing new, edit, and remove functions acting on master tables.
"""


from config import (MASTER_TABLES,
                     MASTER_TABLES_INDICES,
                     CRON_TABLE,
                     CRON_FIELDS,
                     CRON_TYPES,
                     CRON_LOG_TABLE,
                     CRON_LOG_FIELDS,
                     CRON_LOG_TYPES,
                     LOG_TABLE,
                     LOG_FIELDS,
                     LOG_TYPES,
                     USER_TABLE,
                     USER_FIELDS,
                     USER_TYPES,
                     CONNECTION_TABLE,
                     CONNECTION_FIELDS,
                     CONNECTION_TYPES,
                     GLOBAL_TABLE,
                     GLOBAL_FIELDS,
                     GLOBAL_TYPES,
                     MASTER_PATH,
                     PACKAGE_PATH,
                     DATE_FORMAT,
                     MASTER_DB_NAME)
from .util import transform_string_date, clean, broadcast, Path
from .setup import Config, Menu
from .loggers import rLog, cLog
from .setup import task_setup
from .data import BACKENDS, MASTER, Table
from .data.sql import is_db
import datetime
import socket
import pyodbc
import re
import os


class SetupMaster(Config, Menu):
    """
    Master setup class.
    Sets up and stores static multi-session information for ease of use.
    Inherits from sql_i, Menu, and Config classes.
    Changes are coming for this class:
        Code shares functions with Cron.
        Will filter all user input data to exclude sql reserved words and characters.
    """
    filename = 'config.ini'
    white_list = {'user_name': 's',
                  'task_name': 's',
                  'PATH': 's',
                  'task_path': 's',
                  'mode': 's'}

    def __init__(self, user_name=None, path=MASTER_PATH, mode='normal'):
        kwargs = {'user_name': user_name,
                  'PATH': path}
        Config.__init__(self, mode, kwargs)  # usually called as **kwargs, this is simply a dict..?
        Menu.__init__(self)

        # self._global_table = GLOBAL_TABLE
        # self._user_table = USER_TABLE
        # self._connect_table = CONNECTION_TABLE
        # self._log_table = LOG_TABLE
        # self._log_fields = LOG_FIELDS
        # self._cron_table = CRON_TABLE
        # self._cron_fields = CRON_FIELDS
        # self._cron_log_table = CRON_LOG_TABLE
        # self._cron_log_fields = CRON_LOG_FIELDS
        # self._master_tables = MASTER_TABLES

        # TODO: move the initial package setup to setup.__init__? user input is not required to check master tables.
        self._first_init()
        # self.write_path()
        self.index_master_tables()

        self.dformat_example = transform_string_date(dtat=datetime.datetime.now())

        self.task_path = None
        self.check = None

    # ## NEW AND INITIAL FUNCTIONS ###

    def menu(self):
        """
        Starts inherited menu process.
        :return: None
        """
        self.__login()
        self.run_menu(d=main, endfunction=cleanup)

    def __login(self, overwrite=False):
        if not self.user_name or overwrite:
            broadcast(msg="Login:", clamor=0)
            self.user_name = self.get_user(new=False)

    # TODO: REWORK THIS; each object should know how to setup itself. (So any piece can be used in future projects).
    def _first_init(self, cron=False):
        """
        Generates tables if they do not exist.
        This function will be handled by Setup masterclass.
        """
        msg = "Currently reporting master path is '%s'. " \
              "If you would like to change this edit the path in your rpt//path.py file." % MASTER_PATH
        broadcast(msg=msg, clamor=0)
        for _table in MASTER_TABLES:
            while True:
                with Path(MASTER_PATH):
                    if not is_db(database=MASTER_DB_NAME, table=_table):
                        raw = clean(raw_input("Table %s was not found, would you like to run setup for this table "
                                              "now?(Y/N)\n" % _table))
                        if raw == 'y' and not cron:
                            if _table == LOG_TABLE:
                                # rl = RealLogger()
                                self.__new_master(table=LOG_TABLE, fields=LOG_FIELDS,
                                                  values=None, types=LOG_TYPES, if_not=True)
                            elif _table == CRON_LOG_TABLE:
                                # cl = CronLogger()
                                self.__new_master(table=CRON_LOG_TABLE, fields=CRON_LOG_FIELDS,
                                                  values=None, types=CRON_LOG_TYPES, if_not=True)
                            elif _table == GLOBAL_TABLE:
                                self.__new_master(table=GLOBAL_TABLE, fields=GLOBAL_FIELDS,
                                                  values=None, types=GLOBAL_TYPES, if_not=True)
                                self.new_global()
                                break
                            elif _table == USER_TABLE:
                                self.__new_master(table=USER_TABLE, fields=USER_FIELDS,
                                                  values=None, types=USER_TYPES, if_not=True)
                                self.new_user()
                                break
                            elif _table == CONNECTION_TABLE:
                                self.__new_master(table=CONNECTION_TABLE, fields=CONNECTION_FIELDS,
                                                  values=None, types=CONNECTION_TYPES, if_not=True)
                                self.new_connection()
                                break
                            elif _table == CRON_TABLE:
                                self.__new_master(table=CRON_TABLE, fields=CRON_FIELDS,
                                                  values=None, types=CRON_TYPES, if_not=True)
                                break
                            else:
                                print "%s not anticipated as a master table." % _table
                        elif raw == 'n':
                            print "Table %s not setup successfully. \nSetup aborted." % _table
                            break
                        else:
                            print "Invalid Input."
                    else:
                        break

    # ## NEW FUNCTIONS ###
    def new_user(self):
        """
        Generates new users, complete with connections, and updates them to the users table.
        """
        user_name = self.get_user(new=True)
        email = self.get(ask='new default email', possible=None, new=True)
        db_raw = self.get(ask='new default database', possible=None, new=True)
        con_str = self.test_full('Please enter a connection string for %s.\n' % db_raw, odbc=True)
        values = [(user_name, db_raw, con_str, email)]
        self.__new_master(table=USER_TABLE, fields=USER_FIELDS, values=values, types=USER_TYPES, if_not=True)
        rLog.info("User %s created successfully." % user_name)
        print "User %s created successfully." % user_name

    def new_global(self):
        """
        Generates new global connectors and updates them to the globals table.
        """
        http = self.test_full('HTTP Proxy (http://address:port):\n')
        ftp = self.test_full('FTP Proxy (http://address:port):\n')
        smtp = self.test_full('SMTP Server (server:port):\n')
        h1, h2 = self.split_server(http)
        f1, f2 = self.split_server(ftp)
        s1, s2 = self.split_server(smtp)
        values = [('http proxy', http, h1, h2),
                  ('ftp proxy', ftp, f1, f2),
                  ('smtp server', smtp, s1, s2)]
        self.__new_master(table=GLOBAL_TABLE, fields=GLOBAL_FIELDS, values=values, types=GLOBAL_TYPES, if_not=True)
        # rLog.info('Global Settings %s created altered to %s successfully.' % ())

    def new_connection(self):
        """
        Generates new users, complete with connections, and updates them to the users table.
        """
        self.__login()
        db_raw = raw_input("Enter the names of servers to be added for this user separated by commas:\n").split(',')
        values = []
        for db in db_raw:
            db = clean(db).strip()
            con_str = self.test_full('Please enter your connection string for %s.\n' % db, odbc=True)
            values.append((self.user_name, db, con_str))
        self.__new_master(table=CONNECTION_TABLE, fields=CONNECTION_FIELDS,
                          values=values, types=CONNECTION_TYPES, if_not=True)
        rLog.info("Connection(s) %s for user %s created successfully." % (','.join(db_raw), self.user_name))
        print "Connection(s) %s for user %s created successfully." % (','.join(db_raw), self.user_name)

    def _new_task(self):
        """
        Schedules new cron task.
        """
        self.__login()
        self.__new_master(table=CRON_TABLE, fields=CRON_FIELDS, values=None, types=CRON_TYPES, if_not=True)

        possible_values = BACKENDS.select(connect=MASTER, table=CRON_TABLE, fields=['FLAGGED_RUNS'],
                                          where=[('USER_NAME', self.user_name), ])
        task_name = self.get(ask='task name', possible=possible_values, new=True)
        dtat = self.get(ask='new first run date and time (use format %s, ex: %s)' % (DATE_FORMAT, self.dformat_example),
                        possible=None, new=True)
        delta = self._get_delta()
        # delta = self.get(ask='new time between runs', possible=None, new=True)
        load_date = datetime.datetime.now()

        run_end = self.get("a date and time you would like this task to not run after (OPTIONAL, use format %s, "
                           "ex: %s)" % (DATE_FORMAT, self.dformat_example), new=True)
        if run_end == -1:
            run_end = None
        if not run_end:
            run_end = None
        if -1 in [self.user_name, task_name, dtat, delta]:
            return

        if delta:
            values = [(task_name, self.user_name, dtat, None, delta.total_seconds(), load_date, run_end, None, 0, None)]
        else:
            values = [(task_name, self.user_name, dtat, None, delta, load_date, run_end, None, 0, None)]

        yn = clean(raw=raw_input("Would you like to add unusual one time runs to this task? (Y/N)\n"), lower=True)
        if yn == 'y':
            self._add_flagged_run(task_name=task_name)

        self.__new_master(table=CRON_TABLE, fields=CRON_FIELDS, values=values, types=CRON_TYPES, if_not=True)
        task_setup(task_name, user=self.user_name)
        self.task_name = task_name
        print "Task %s created successfully." % task_name
        rLog.info("Task %s created successfully." % task_name)

    def _get_delta(self):
        """
        gets task delta from user
        :return: timedelta object
        """
        while True:
            mode = clean(raw_input("""Enter Run Type, D for daily, W for weekly, DIM for day in month, or enter a time
            interval in the following format (weeks, days, hours, minutes):\n"""))
            delta = self.__get_delta(mode)
            if delta or mode == 'dim':
                self.runtype = mode
                self.delta = delta
                return delta
            else:
                print "Input invalid.\n"

    @staticmethod
    def __get_delta(mode):
        """
        Returns parsed timedelta from run mode. Returns None for day in month.
        """
        if mode:
            m = re.search('\(*\d+,\d+,\d+,\d+\)*', mode)
            if mode == 'd':
                return datetime.timedelta(days=1)
            elif mode == 'w':
                return datetime.timedelta(weeks=1)
            elif mode == 'dim':
                return None
            elif m:
                m = [int(x.replace('(', '').replace(')', '')) for x in m.group().split(',')]
                parsed = datetime.timedelta(weeks=m[0], days=m[1], hours=m[2], minutes=m[3])
                return parsed
        else:
            print "Invalid input."
            return None

    def _add_flagged_run(self, task_name=None):
        """
        Processes a list of date strings and add a new entry.
        :param task_name: task_name to go with self.user as primary key for cron table.
        :return: None
        """
        # get flagged run date and time
        dtat = self.get(ask='new first run date and time', possible=None, new=True)
        # get current flagged list
        sql = 'SELECT FLAGGED_RUNS FROM CRON WHERE TASK_NAME = ? AND USER_NAME = ?'
        values = [task_name, self.user_name]
        current = BACKENDS.pass_sql(connect=MASTER, sql=sql, values=values)
        if current:
            # convert to list object if not None
            current = eval(current[0][0])
            # convert all data_objects to datetime data_objects for comparison
            current = [transform_string_date(x) for x in current]
        else:
            # if None, set to empty list
            current = []
        # add user defined date and time
        current.append(dtat)
        # sort the list before re-entry
        current.sort()
        # convert entries back to strings
        current = [transform_string_date(x) for x in current]

        # write string of list to table
        BACKENDS.replace_rows(connect=MASTER, table=CRON_TABLE, setfield='FLAGGED_RUNS', setvalue=str(current),
                              fields=['TASK_NAME', 'USER_NAME'], values=(task_name, self.user_name), ctype='local')

    # ## CREATE MASTER TABLE FUNCTIONS ###
    def __new_master(self, table, fields, values, types, if_not=True):
        """
        Makes a new master table and inserts values.
        :param table: Name of master table to add.
        :param fields: List of field names.
        :param types: List of field SQLite3 data types.
        :param values: List of value tuples (matching fields[1:] since int key auto-increments).
        :param if_not: When True will not overwrite existing table.
        :return: None
        """
        # TODO: on failure drop master table.
        # try:
        BACKENDS.create_table(connect=MASTER, table=table, fields=fields, data_types=types, if_not=if_not)
        rLog.info('Create table %s successful.' % table)
        if values:
            BACKENDS.insert_rows(connect=MASTER, table=table, fields=fields[1:], rows=values)
            rLog.info('Insert into %s successful.' % table)
        # except:
        #     BACKENDS.drop_table(connect=MASTER, table=table)

    # ## CHECK FUNCTIONS ###
    def test_full(self, format_string=None, odbc=False):
        """
        Tests odbc or socket connections. Keeps user in menu until
        they accept a failure or achieve a connection success.
        """
        while True:
            raw = raw_input(format_string)
            if odbc:
                flg = self.test_odbc(raw)
            else:
                flg = self.test_socket(raw)
            if flg:
                print "Connection test successful."
                return raw
            else:
                yn = clean(raw_input("No connection could be made to %s. Would you like to try again? (Y/N)\n" % raw),
                           lower=False, sql=False, length_limit=False)
                if yn.lower() == 'y':
                    pass
                elif yn.lower() == 'n':
                    return raw
                else:
                    print "Invalid input."

    # tests an address connection with socket
    def test_socket(self, string):
        """
        Accepts a string of format 'host:port'.
        Parses string and tests via socket.socket() method.
        Returns True if successful or False if unsuccessful.
        """
        soc = socket.socket()
        try:
            host, port = self.split_server(string)
            try:
                soc.connect((host, port))
                return True
            except Exception, e:
                print "Connection Error: %s" % e
                return False
        except Exception, e:
            print "Invalid address input: %s.\n Error:%s" % (string, e)
            return False
        finally:
            soc.close()

    # tests an odbc connection string with pyodbc
    @staticmethod
    def test_odbc(string):
        """
        Accepts an odbc connection string.
        Accepts to connect via pyodbc.
        Returns True if successful or False if unsuccessful.
        """
        try:
            conn = pyodbc.connect(string)
            conn.close()
            return True
        except Exception, e:
            print "Connection Error: %s" % e
            return False

    @staticmethod
    def split_server(string):
        """ Parses server strings intelligently using regex and converts port to int type. """
        try:
            host, port = re.search("^(.+):(\d+)$", string).groups()
            port = int(port)
            return host, port
        except Exception, e:
            print "Error parsing server string: %s" % e
            return None, None

    # ## GLOBAL EDIT FUNCTIONS ###

    def _edit_http(self):
        """
        Adds a server address and port to globals table using checks.
        :return: None
        """
        http = self.test_full('Enter New HTTP Proxy (http://address:port):\n')
        h1, h2 = self.split_server(http)
        values = [('http proxy', http, h1, h2), ]
        BACKENDS.drop_rows(connect=MASTER, table=GLOBAL_TABLE, where=[('TYPE', 'http proxy')])
        BACKENDS.insert_rows(connect=MASTER, table=GLOBAL_TABLE, rows=values, fields=GLOBAL_FIELDS[1:])
        rLog.info('HTTP changed to %s.' % http)

    def _edit_ftp(self):
        """
        Adds a server address and port to globals table using checks.
        :return: None
        """
        ftp = self.test_full('Enter New FTP Proxy (http://address:port):\n')
        f1, f2 = self.split_server(ftp)
        values = (('ftp proxy', ftp, f1, f2),)
        BACKENDS.drop_rows(connect=MASTER, table=GLOBAL_TABLE, where=[('TYPE', 'ftp proxy')])
        BACKENDS.insert_rows(connect=MASTER, table=GLOBAL_TABLE, rows=values, fields=GLOBAL_FIELDS[1:])
        rLog.info('FTP changed to %s.' % ftp)

    def _edit_smtp(self):
        """
        Adds a server address and port to globals table using checks.
        :return: None
        """
        smtp = self.test_full('Enter New SMTP Server (server:port):\n')
        s1, s2 = self.split_server(smtp)
        values = (('smtp server', smtp, s1, s2),)
        BACKENDS.drop_rows(connect=MASTER, table=GLOBAL_TABLE, where=[('TYPE', 'smtp server')])
        BACKENDS.insert_rows(connect=MASTER, table=GLOBAL_TABLE, rows=values, fields=GLOBAL_FIELDS[1:])
        rLog.info('SMTP changed to %s.' % smtp)

    # ## SPECIALIZED FUNCTIONS ###

    @staticmethod
    def write_path():
        """
        Writes the PATH string to rpt directory on setup. This PATH is used as master PATH in the rest of the package
        via config.
        :return: None
        """
        with open(PACKAGE_PATH+'\\path.py', 'w') as to:
            to.write('PATH = r"%s"\nBACKUP_PATH = r""' % os.getcwd())

    def index_master_tables(self):
        """
        Indexes all master tables on the key held in config.
        :return: None
        """
        if is_db(MASTER_DB_NAME):
            for key, value in MASTER_TABLES_INDICES.items():
                unique = value[0]
                fields = value[1:]
                try:
                    BACKENDS.create_index(connect=MASTER, index=key+'_DEFAULT_INDEX', table=key,
                                          fields=fields, unique=unique)
                    rLog.info('Master table %s indexed.' % key)
                except:
                    pass

    def get_user(self, new=False):
        """
        Gets user name from user. Basic login.
        :param new: If this is to be a new user name.
        :return: user_name
        """
        try:
            possible_users = BACKENDS.select(connect=MASTER, table=USER_TABLE, fields=['USER_NAME'])[0]
        except IndexError:
            possible_users = ''
        ask = 'user name'
        if new:
            ask = 'new user name'
        return self.get(ask=ask, possible=possible_users, new=new)

    def __edit_user_name_sub(self, new_user_name):
        """
        Replaces user_name in cron, connections, and user tables on edit.
        :param new_user_name: user_name to change to.
        :return: None
        """
        BACKENDS.update_values(connect=MASTER, table=USER_TABLE, where=[('USER_NAME', self.user_name), ],
                               sets=[('USER_NAME', new_user_name), ])
        BACKENDS.update_values(connect=MASTER, table=CONNECTION_TABLE, where=[('USER_NAME', self.user_name), ],
                               sets=[('USER_NAME', new_user_name), ])
        BACKENDS.update_values(connect=MASTER, table=CRON_TABLE, where=[('USER_NAME', self.user_name), ],
                               sets=[('USER_NAME', new_user_name), ])
        rLog.info('User %s renamed to %s in tables %s, %s, %s.' % (self.user_name, new_user_name,
                                                                      USER_TABLE, CONNECTION_TABLE, CRON_TABLE))

    # ## GENERIC FUNCTION REWORKS ###

    # rework
    def create(self, new):
        """
        Creates new table and entry for each table type.
        :param new: key word for what to create, 'task', 'connection', or 'user'.
        :return: None
        """
        if new == 'task':
            self._new_task()
        elif new == 'connection':
            self.new_connection()
        elif new == 'user':
            self.new_user()
        else:
            broadcast(msg='Invalid item for creation.', clamor=1)
        # rLog.info("Entry where %s removed from %s." % (, ))

    def remove(self, args):
        """
        Removes entries from master tables.
        :param args: dictionary containing arguments for table entry removal.
        :return: None
        """
        # setup variables
        table = args['table']
        index_fields = MASTER_TABLES_INDICES[table][1:]
        index_values = [self.get(ask=x, possible=None, new=True) for x in args['index_value_asks']]
        # index_values.reverse()

        broadcast(msg='table: %s, index_fields: %s, index_values: %s' % (table, index_fields, index_values), clamor=7)

        # drop entries matching index values
        l = []
        for item in zip(index_fields, index_values):
            l.append(' = '.join(list(item)))
        log_string = ', '.join(l)
        BACKENDS.drop_rows(connect=MASTER, table=table, where=zip(index_fields, index_values))
        rLog.info("Entries where %s removed from %s." % (log_string, table))
        broadcast(msg="Entries where %s removed from %s." % (log_string, table), clamor=1)
        if table == USER_TABLE:
            print 'Re-Login:'
            self.get_user(new=False)

    def edit(self, args):
        """
        Replaces values in master tables.
        :param args: dictionary containing arguments for table entry replacement.
        :return: None
        """
        # setup #
        table = args['table']
        set_field = args['set_field']
        ask = args['ask']
        index_asks = args['index_value_asks']
        index_fields = MASTER_TABLES_INDICES[table][1:]

        # get possible values as a list of tuples #
        if 'USER_NAME' in index_fields:
            possible_values = [row for row in BACKENDS.select(connect=MASTER, table=table, fields=index_fields,
                                                              where=[('USER_NAME', self.user_name)])]
        else:
            possible_values = [row for row in BACKENDS.select(connect=MASTER, table=table, fields=index_fields)]

        # get index values as a list of values #
        index_values = [self.get(ask=x, possible=zip(*possible_values)[i], new=False) for i, x in enumerate(index_asks)]

        # exit this function if no values to edit or user does not own the requested item #
        if -1 in index_values:
            return

        # display current value for field to set #
        current = BACKENDS.select(connect=MASTER, table=table, fields=[set_field], where=zip(index_fields, index_values))[0][0]
        display_string = ' '.join(ask.split(' ')[1:])
        broadcast(msg='currently %s is %s.' % (display_string, current), clamor=1)

        # get new value from user #
        set_value = self.get(ask=ask, possible=possible_values, new=True)

        # write new value #
        if set_field.lower() == 'user_name':
            self.__edit_user_name_sub(new_user_name=set_value)
            table = ', '.join([USER_TABLE, CRON_TABLE, CONNECTION_TABLE])
            self.user_name = set_value
        else:
            BACKENDS.update_values(connect=MASTER, table=table, where=zip(list(index_fields), list(index_values)),
                                   sets=[(set_field, set_value), ])
        rLog.info('%s %s altered to %s in table(s) %s.' % (display_string, current, set_value, table))
        broadcast(msg='%s %s altered to %s in table(s) %s.' % (display_string, current, set_value, table), clamor=1)

    def get(self, ask, possible=None, new=False):
        """
        Get values from users. Possible is a list of current entries in selected table for field to be edited. New is
        an indicator if exclusion or inclusion of possible fields. If 'new' and not 'possible' pass through any value.
        :param ask: Field, as string, to ask user for.
        :param possible: List of current values matching selected field and table.
        :param new: True and value in possible:
                        disallow value, prompt to try again.
                    True and not possible:
                        pass through user value.
                    False and possible:
                        pass through user value.
                    False and not possible:
                        display 'no possible values' string and exit.
        :return: user's answer to 'please enter <ask>:' or -1. If -1 get calling functions end.
        """
        broadcast(msg='ask: %s, possible: %s, new: %s' % (ask, possible, new), clamor=7)
        if ask.lower() == 'user_name':
            value = self.user_name
        else:
            while True:
                value = clean(raw_input('please enter '+ask+':\n'))
                # print value
                if new and possible:
                    display_sting = ' '.join(ask.split(' ')[1:])
                    if value in possible:
                        y_n = raw_input('%s %s is already taken. Would you like to try again? (Y/N)\n'
                                        % (display_sting, value))
                        if y_n.lower() == 'n':
                            value = -1
                            break
                    else:
                        break
                elif possible:
                    display_sting = ask
                    if value not in possible:
                        y_n = raw_input('%s %s not found as a valid entry. Would you like to try again? (Y/N)\n'
                                        % (display_sting, value))
                        if y_n.lower() == 'n':
                            value = -1
                            break
                    else:
                        break
                elif new:
                    break
                else:
                    broadcast(msg='No values yet in table.', clamor=1)
                    value = -1
                    break
        return_value = clean(raw=value)
        try:
            if return_value not in locals():
                return_value = eval(return_value)
        except Exception:
            pass
        # print return_value
        return return_value

    def view(self, table):
        """
        prints table
        :param table: table name as string
        :return: None
        """
        try:
            t = Table.from_select(connect=MASTER, table_name=table, where=[('USER_NAME', self.user_name), ])
        except Exception:  # sqlite3 OperationalError if no USER_NAME column in table (like GLOBALS_TABLE).
            t = Table.from_select(connect=MASTER, table_name=table)
        t.min_print_length += 5
        print t

# ## MENUS ###
"""
Uses SetupMaster's inherited Menu class to build menu element connections. Passed arguments and table associations are
predefined from config imports.
"""

# ## MENU TABLE ASSOCIATIONS
a_table = CRON_TABLE
b_table = USER_TABLE
c_table = CONNECTION_TABLE
d_table = GLOBAL_TABLE

# ## MENU BASE ARGUMENTS ###
a_args = {'table': a_table,
          'index_value_asks': ['user_name', 'task_name']}
b_args = {'table': b_table,
          'index_value_asks': ['user_name']}
c_args = {'table': c_table,
          'index_value_asks': ['user_name', 'database_name']}
d_args = {'table': d_table,
          'index_value_asks': ['server type']}

# ## MENU SETUPS ###

# ## A SUB-MENU ITEMS ###
aa = {'name': 'aa',
      'type': 'function',
      'printed': 'create new task',
      # 'funct': 'self.create(new="user")',
      'funct': 'self._new_task()'}
ab = {'name': 'ab',
      'type': 'function',
      'printed': 'remove task',
      'funct': 'self.remove(args=%s)' % a_args}
ac_args = {'set_field': 'task_name',
           'ask': 'new task_name'}
ac_args.update(a_args)
ac = {'name': 'ac',
      'type': 'function',
      'printed': 'edit task_name',
      'funct': 'self.edit(args=%s)' % ac_args}
ad_args = {'set_field': 'next_run',
           'ask': 'new next run'}
ad_args.update(a_args)
ad = {'name': 'ad',
      'type': 'function',
      'printed': 'edit next run',
      'funct': 'self.edit(args=%s)' % ad_args}
ae_args = {'set_field': 'delta',
           'ask': 'new time between runs'}
ae_args.update(a_args)
ae = {'name': 'ae',
      'type': 'function',
      'printed': 'edit time between runs',
      'funct': 'self.edit(args=%s)' % ae_args}
af_args = {'set_field': 'run_end_dt',
           'ask': 'new date to not run after'}
af_args.update(a_args)
af = {'name': 'af',
      'type': 'function',
      'printed': 'edit date to not run after',
      'funct': 'self.edit(args=%s)' % af_args}
ag_args = {'set_field': 'flagged_runs',
           'ask': 'new non-standard run times'}
ag_args.update(a_args)
ag = {'name': 'ah',
      'type': 'function',
      'printed': 'edit non-standard run times',
      'funct': 'self.edit(args=%s)' % ag_args}
ah = {'name': 'ah',
      'type': 'function',
      'printed': 'view tasks',
      'funct': 'self.view(table="%s")' % a_table}

# ## B SUB-MENU ITEMS ###
ba = {'name': 'ba',
      'type': 'function',
      'printed': 'create new user',
      # 'funct': 'self.create(new="user")',
      'funct': 'self.new_user()'}
bb = {'name': 'bb',
      'type': 'function',
      'printed': 'remove this user',
      'funct': 'self.remove(args=%s)' % b_args}
bc_args = {'set_field': 'user_name',
           'ask': 'new user_name'}
bc_args.update(b_args)
bc = {'name': 'bc',
      'type': 'function',
      'printed': 'edit user_name',
      'funct': 'self.edit(args=%s)' % bc_args}
bd_args = {'set_field': 'default_database',
           'ask': 'new default database_name'}
bd_args.update(b_args)
bd = {'name': 'bd',
      'type': 'function',
      'printed': 'edit default database_name',
      'funct': 'self.edit(args=%s)' % bd_args}
be_args = {'set_field': 'default_connection_string',
           'ask': 'new default connection string'}
be_args.update(b_args)
be = {'name': 'be',
      'type': 'function',
      'printed': 'edit default connection string',
      'funct': 'self.edit(args=%s)' % be_args}
bf_args = {'set_field': 'default_email',
           'ask': 'new default email address'}
bf_args.update(b_args)
bf = {'name': 'bf',
      'type': 'function',
      'printed': 'edit default email address',
      'funct': 'self.edit(args=%s)' % bf_args}
bg = {'name': 'bg',
      'type': 'function',
      'printed': 'view user defaults',
      'funct': 'self.view(table="%s")' % b_table}

# ## C SUB-MENU ITEMS ###
ca = {'name': 'ca',
      'type': 'function',
      'printed': 'create new database connection',
      # 'funct': 'self.create(new="connection")',
      'funct': 'self.new_connection()'}
cb = {'name': 'cb',
      'type': 'function',
      'printed': 'remove database connection',
      'funct': 'self.remove(args=%s)' % c_args}
cc_args = {'set_field': 'database',
           'ask': 'new database_name'}
cc_args.update(c_args)
cc = {'name': 'cc',
      'type': 'function',
      'printed': 'edit database_name',
      'funct': 'self.edit(args=%s)' % cc_args}
cd_args = {'set_field': 'connection_string',
           'ask': 'new connection string'}
cd_args.update(c_args)
cd = {'name': 'cd',
      'type': 'function',
      'printed': 'edit connection string',
      'funct': 'self.edit(args=%s)' % cd_args}
ce = {'name': 'ce',
      'type': 'function',
      'printed': 'view database connections',
      'funct': 'self.view(table="%s")' % c_table}

# ## D SUB-MENU ITEMS ###
da = {'name': 'dc',
      'type': 'function',
      'printed': 'edit smtp server',
      'funct': 'self._edit_smtp()'}
db = {'name': 'da',
      'type': 'function',
      'printed': 'edit http proxy server',
      'funct': 'self._edit_http()'}
dc = {'name': 'db',
      'type': 'function',
      'printed': 'edit ftp proxy server',
      'funct': 'self._edit_ftp()'}
dd = {'name': 'dd',
      'type': 'function',
      'printed': 'view globals',
      'funct': 'self.view(table="%s")' % d_table}

# ## MAIN MENU ITEMS ###
a = {'name': 'sub a',
     'type': 'menu',
     'printed': 'cron',
     'root': False,
     'funct': [aa, ab, ac, ad, ae, af, ag, ah]}  # sub menu
b = {'name': 'sub b',
     'type': 'menu',
     'printed': 'user',
     'root': False,
     'funct': [ba, bb, bc, bd, be, bf, bg]}  # sub menu
c = {'name': 'sub c',
     'type': 'menu',
     'printed': 'connections',
     'root': False,
     'funct': [ca, cb, cc, cd, ce]}  # sub menu
d = {'name': 'sub d',
     'type': 'menu',
     'printed': 'globals',
     'root': False,
     'funct': [da, db, dc, dd]}  # sub menu

# ## MAIN MENU ###
main = {'name': 'main',
        'type': 'menu',
        'printed': 'main',
        'root': True,
        'funct': [a, b, c, d]}  # main menu

# ## MENU CLEANUP FUNCTION ###
cleanup = 'str("Exit function called.")'
