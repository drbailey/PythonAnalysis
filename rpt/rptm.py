__author__  = 'drew bailey'
__version__ = 0.93

"""
Reporting Master Classes.
Master handler classes for all other packages in rpt module.
Accepts data, process, merge, and distribution parameters as inputs and generates and executes a reporting process.
Can execute (but is not limited to) the following primary functions:
    1.) Pull data from local SQL sources via SQLite.
    2.) Pull data from external SQL sources via ODBC.
    3.) Pull data from excel.
    4.) Pull data from regex parsed text based files.
    5.) Pull data from Outlook attachments or mail data_objects.
    6.) Run VBA on or in files.
    7.) Write to csv, xlsx, or xlsm file simple_type. (or standard txt by rows)
    8.) Backup files in local database, xlsx, xlsm, or csv files.
    9.) Merge data in memory with any SQL join type.
    10.)Insert information into existing report templates.
    11.)Distribute files via email SMTP or move to file PATH.
    12.)Thoroughly log above functions.
    13.)Send confirmation emails to the logged in user.
"""

# from .loggers import onError
from .config import MASTER_PATH, VOLUME, DATE_LY_SQL, DATE_FOLDER_FORMAT, DIV_0, DATE_FORMAT_SHORT
from .data import Library, Table, Parser, ExcelWriterV2, OutlookData, explicit_py_type, BACKENDS, MASTER
from .util import Path, ambiguous_extension, collect_garbage, broadcast, get_date
from .com import default_copy, default_range_to_image
from .distribution import Eml, rwc_file
from .setup import Config
import datetime
import warnings
import time
import os


DEFAULTS = {
    '': '',
    }


class RptMaster(Config):
    """
    Reporting Master is the Superclass for RptAdv, RptStd
    expected:
        :param user_name:\tuser_name as string.
        :param task_name:\ttask name as string, should match cron table task.
        :param run_state:\taccepted run_states are as follows-
            lmon :\tlast monday
            ltue :\tlast tuesday
            lwed :\tlast wednesday
            lthur :\tlast thursday
            lfri :\tlast friday
            lsat :\tlast saturday
            lsun :\tlast sunday
            plusten :\tten days from now
            else :\tyesterday.
        :param e_db:\tdatabase name as string.
        :param data_parameters: Inputs for get data methods of form [(source, detail, variables), ]
        :param merge_parameters: Inputs for merge methods of form [(data indexes, on how), ]
        :param process_parameters: Inputs for process methods (VBA, write, backup, ect...) in dictionary format.
            Looks for:
                'write_data',
                'ext',
                'write_header',
                'backup_data',
                'backup_database',
                'write_date_data',
                'write_opacity',
                'write_locations' -> {data_sheet_name_or_index: (to_sheet_name, to_row, to_column)}
                'template'
        :param distribution_parameters: Inputs for distribution process methods (distribute to PATH, email, ect...) in
            dictionary format.
            Looks for:
                'sender',
                'to',
                'cc',
                'bcc',
                'recipients',
                'subject',
                'message',
                'password',
                'embed',
                'embedh',
                'body_image',
                'distro_path',
                'date_folder',
                'files',
                'confirm',
        :param rundate_f: An end date for this year's data pull methods, used heavily for default date parameters.
        :param ly_f: An end date for last year's data pull methods.
        :param rundate_i: A start date for this year's data pull methods.
        :param ly_i: A start date for last year's data pull methods.
        :param kwargs: Anything else can be passed and will be initialized as a self variable of the same name.

    Uncommonly used variables:
        :param local_db: The name of the local database to get information from and to backup to.
        :param task_path: If an uncommon task PATH is necessary.
    """
    # configfile = {'config.ini'}
    section = 'TASK'
    '''i for int, f for float, b for boolean, d for date, s for string, else input type.'''
    white_list = {'task_name': 's',
                  'sub_task': 's',
                  'user_name': 's',
                  'run_state': 's',
                  # 'e_db': 's',  # USED FOR INTERNAL FUNCTIONS, LY, BI...
                  'MASTER_PATH': 's',
                  'task_path': 's',

                  'data_parameters': 'list',
                  'merge_parameters': 'list',
                  'process_parameters': 'dict',
                  'distribution_parameters': 'dict',

                  # Set All Dates to None w/ Factory #
                  'rundate_f': 's',
                  'ly_f': 's',
                  'rundate_i': 's',
                  'ly_i': 's',
                  }

    def __init__(self, mode='normal', **kwargs):
        # ## setup non-None input derived variables ###
        # self.local_db = LOCAL_DB
        self.user_name = ''
        self.task_name = ''
        self._sub_task = ''
        self.run_state = 'now'
        self.raw = []
        self.merged = []
        self.data_limit = 0
        self.files = True
        self.data_parameters = []
        self.merge_parameters = []
        self.process_parameters = {}
        self.distribution_parameters = {}
        # necessary for sub_task property
        self.files = []
        self.ext = ''

        # ## initialize default date variables ###
        # true run datetime
        self.now = datetime.datetime.now()
        # dates to run as
        self.rundate_f = None
        self.rundate_i = None
        self.ly_f = None
        self.ly_i = None

        # ## setup path defaults ###
        self.task_path = os.getcwd()
        self.master_path = MASTER_PATH

        # ## factory init and fill local variables from Config
        # Config.__init__(self, mode, kwargs)
        super(RptMaster, self).__init__(mode, kwargs)

        # ## back to master ###
        os.chdir(self.master_path)

        # ## setup sql connections ###
        self.sqlib = Library(user_name=self.user_name, path=self.task_path)
        self.backends = BACKENDS

        # ## establish run dates ###
        if isinstance(self.run_state, basestring):
            self.run_mode(flag=self.run_state)
        else:
            self.run_mode(start=self.run_state[0], end=self.run_state[1])

        # ## setup full task name ###
        # if self.sub_task:
        #     self.full_task = self.task_name+' '+self.sub_task
        # else:
        #     self.full_task = self.task_name

        # ## unpack grouped parameters as needed... ###

        # ## unpack data parameters ###

        # ## unpack process parameters ###
        # # general ##
        self.write_data = self.process_parameters.get('write_data', True)
        self.ext = self.process_parameters.get('ext', '.xls*')
        # self.write_mode = self.process_parameters.get('write_mode', 'complex')  # simple (openpyxl) / complex (com)
        # self.hide_sheets = self.process_parameters.get('hide_sheets', True)
        # self.range_start = self.process_parameters.get('range_start', (1, 1))
        self.write_header = self.process_parameters.get('write_header', True)
        self.backup_data = self.process_parameters.get('backup_data', True)
        self.backup_database = self.process_parameters.get('backup_database', 'local')  # changed from 'master'
        self.write_date_data = self.process_parameters.get('write_date_data', False)
        # self.excel_date = self.process_parameters.get('excel_date', True)
        self.write_opacity = self.process_parameters.get('write_opacity', 4)
        self.write_locations = self.process_parameters.get('write_locations', {})
        self.collapse = self.process_parameters.get('collapse', True)

        # ## com ##
        # self.com_type = self.process_parameters.get('com_type', None)
        # self.com_path = self.process_parameters.get('com_path', self.task_path)
        # self.module = self.process_parameters.get('module', 'DataProcessor')
        # self.macro = self.process_parameters.get('macro', 'Processor')
        # # both ##
        self.template = self.process_parameters.get('template', r'Template_%s%s' % (self.full_task, self.ext))

        # ## unpack distribution parameters ###
        # # path ##
        self.distro_path = self.distribution_parameters.get('distro_path', None)
        self.date_folder = self.distribution_parameters.get('date_folder', True)
        # # email ##
        self.sender = self.distribution_parameters.get('sender', None)
        self.subject = self.distribution_parameters.get('subject', 'NW Reporting - %s' % self.full_task)
        self.message = self.distribution_parameters.get('message', '<br>thanks, <br>%s' % self.user_name)
        self.to = self.distribution_parameters.get('to', [])
        self.cc = self.distribution_parameters.get('cc', [])
        self.bcc = self.distribution_parameters.get('bcc', [])
        self.recipients = self.distribution_parameters.get('recipients', [])
        self.password = self.distribution_parameters.get('password', None)
        self.embed = self.distribution_parameters.get('embed', None)
        self.embedh = self.distribution_parameters.get('embedh', None)
        self.body_image = self.distribution_parameters.get('body_image', None)
        # # shared ##
        self.files = self.distribution_parameters.get('files', True)
        self.confirm = self.distribution_parameters.get('confirm', True)

        # ## get unexpected parameter entries for debugging ###
        self._load_unexpected_parameters()

        # ## defaults based on input ###
        if not self.sender and self.user_name:
            self.sender = self.sqlib.get_user_defaults()[2]

        self.source = None

        # Ensure self.files is not bool #
        self.sub_task = self.sub_task

        # ## broadcast ###
        broadcast(msg=DIV_0, clamor=1)
        broadcast(msg=self.full_task, clamor=1)
        broadcast(msg='run date set to: %s' % self.rundate_f, clamor=2)
        broadcast(msg=DIV_0, clamor=1)

    @property
    def full_task(self):
        if self.sub_task:
            return self.task_name+' - '+self.sub_task
        return self.task_name

    @property
    def sub_task(self):
        return self._sub_task

    @sub_task.setter
    def sub_task(self, value):
        file_string = '%s %s%s'
        f_ = file_string % (self.full_task, self.rundate_f, self.ext)
        self._sub_task = value
        f = file_string % (self.full_task, self.rundate_f, self.ext)
        if self.files is True:
            self.files = [f]
        elif f_ in self.files:
            self.files[self.files.index(f_)] = f

    @sub_task.deleter
    def sub_task(self):
        self.sub_task = ''

    # ## initialization functions ###
    def run_mode(self, flag=None, start=None, end=None):
        """
        Input key word, start, or end dates.
        1) Key word for auto run.
        ex) flag='ltue'
        ex) start='2000-01-01', end='2000-01-03'
        *****************************************************************
        Full list of key words:
        lmon : last monday
        ltue : last tuesday
        lwed : last wednesday
        lthur : last thursday
        lfri : last friday
        lsat : last saturday
        lsun : last sunday
        plusten : ten days from now
        else 'now' is yesterday, (entering 'now' does the same without an error printed).

        LY works best with an external connection
        """
        self.ROOTPATH()
        if flag:
            self.rundate_f = self.find_now(flg=flag)
            self.ly_f = self.get_last_year(dt=self.rundate_f)
        elif end or start:
            if end:
                adate, atype = explicit_py_type(end)
                self.rundate_f = adate.date()
                self.ly_f = self.get_last_year(dt=self.rundate_f)
            if start:
                adate, atype = explicit_py_type(start)
                self.rundate_i = adate.date()
                self.ly_i = self.get_last_year(dt=self.rundate_i)
        elif VOLUME > 0:
            warnings.warn('Not enough run date information.', RuntimeWarning)

    def find_now(self, flg):
        """
        Runs as through it were a prior or future date. Useful because of database update times.
        :param flg: Run type flag
        :return: Date object corresponding to flg.
        """
        now = datetime.datetime.now()
        if flg == 'lmon':
            now = self.__get_ldow(0)
        elif flg == 'lsun':
            now = self.__get_ldow(1)
        elif flg == 'lsat':
            now = self.__get_ldow(2)
        elif flg == 'lfri':
            now = self.__get_ldow(3)
        elif flg == 'lthur':
            now = self.__get_ldow(4)
        elif flg == 'lwed':
            now = self.__get_ldow(5)
        elif flg == 'ltue':
            now = self.__get_ldow(6)
        elif flg == 'plusten':
            now = now + datetime.timedelta(10)
        elif flg == 'now':
            now = now - datetime.timedelta(1)
        else:
            now = now - datetime.timedelta(1)
            if VOLUME > 0:
                warnings.warn('Invalid run state, now set to %s.' % now.date(), RuntimeWarning)
        return now.date()

    def get_last_year(self, dt, string=False, from_sql=False):
        """
        Gets last year's date as datetime.date or str from database if user default exists else assumes 365 days ago.
        LY code is specified by user on package setup.
        Sets path to ROOTPATH().
        """
        if from_sql:
            self.ROOTPATH()
            if not self.sqlib.econ:
                ly = get_date() - datetime.timedelta(days=364)
                warnings.warn("No default external database detected. Last year's date set to %s." % ly)  # broadcast?
            else:
                ly = self.backends.pass_sql(connect=self.sqlib.ec, sql=DATE_LY_SQL, values=dt)[0][0]
        else:
            ly = dt - datetime.timedelta(days=364)
        if string:
            return ly.strftime(DATE_FORMAT_SHORT)
        return ly

    def _load_unexpected_parameters(self):
        """
        Loads unexpected parameter keys into a list that is included in confirmation emails.
        :return: None
        """
        process_expected_parameters = [
            'write_data',
            'ext',
            'write_header',
            'backup_data',
            'backup_database',
            'write_date_data',
            'write_opacity',
            'write_locations',
            'template',
            'collapse',
            ]
        distribution_expected_parameters = [
            'sender',
            'to',
            'cc',
            'bcc',
            'recipients',
            'subject',
            'message',
            'password',
            'embed',
            'embedh',
            'body_image',

            'distro_path',
            'date_folder',

            'files',

            'confirm',
            ]

        self.unexpected_parameters = []
        for key in self.process_parameters:
            if key not in process_expected_parameters:
                self.unexpected_parameters.append(key)
        for key in self.distribution_parameters:
            if key not in distribution_expected_parameters:
                self.unexpected_parameters.append(key)
        if self.unexpected_parameters:
            broadcast(msg="Unexpected parameter(s) found: '%s'." % "', '".join(self.unexpected_parameters), clamor=1)

    def TASKPATH(self):
        """ Changes current working directory to task PATH."""
        os.chdir(self.task_path)

    def ROOTPATH(self):
        """ Changes current working directory to the master PATH."""
        os.chdir(self.master_path)

    # ## auto run functions ###

    # switch to balanced ternary? (-1, 0, 1)
    # would allow for user input (True (1) or False (-1)) to alter auto run state (0) of other methods.
    def run(self, get=True, merge=True, process=True, distribute=True):
        """
        Simplifies and groups primary report run methods.
        Sets to TASKPATH().
        :param get: If True executes get_data method.
        :param merge: If True executes merge methods.
        :param process: If True executes process methods.
        :param distribute: If True executes distribution methods.
        :return: None
        """
        broadcast(msg="Run method called on task '%s'." % self.full_task, clamor=1)
        broadcast(msg='Get:\t\t%r\nMerge:\t\t%r\nProcess:\t%r\nDistribute:\t%r' %
                      (get, merge, process, distribute), clamor=2)
        broadcast(msg=DIV_0, clamor=1)
        start = time.time()
        # try:
        if self.data_parameters:
            if get is True:
                self.get()
        if self.merge_parameters and self.raw:
            if merge is True:
                self.merge()
        if process is True:
            self.process()
        if distribute is True:
            self.distribute()
        elapsed = time.time()-start
        broadcast(msg='Run method complete, {0:.1f} seconds elapsed.'.format(elapsed), clamor=1)
        broadcast(msg=DIV_0, clamor=1)
        # except:
        #     onError()
        # finally:
        #     self.close_all()
        self.TASKPATH()

    # ## high level run functions ###
    def get(self, data_parameters=None):
        """
        Simplifies getting data with default methods. Appends data to self.raw.
        Data source names are case insensitive.
        Sets to ROOTPATH().
        :param data_parameters: Accepts object of type [(source, detail, variables), ]
        :return None
        """
        # try:
        self.ROOTPATH()
        if not data_parameters:
            data_parameters = self.data_parameters
        data_list = []
        servers = self.sqlib.get_user_connections()
        server_names = [srv[0].lower() for srv in servers] + self.sqlib.header
        for i, (source, detail, variables) in enumerate(data_parameters):
            data_name = 'Data%i' % i
            if source.lower() in server_names:
                # setup for default sql pull #
                index = server_names.index(source)
                con_name, con_str = None, None
                if index in range(len(servers)):
                    con_name, con_str = servers[index][:2]

                detail = detail % self.__handle_replacer(variables)

                # flag last source in self #
                self.source = source

                # create connection #
                if source in self.sqlib.header:
                    server = self.sqlib[source]
                else:
                    self.sqlib.connect(ctype=con_str, name=con_name)
                    server = self.sqlib[-1]

                # append data #
                data = Table.from_sql(connect=server.c, name=data_name, sql=detail, values=None, parent=server)
                data_list.append(data)
            # todo: Fix.
            elif 'parser' in source:
                plug, app = source.split('_')
                data = Parser.__plugin__(plugin_name=plug, app=app).auto_parse()
                data_list.append(data)
            # todo: Fix.
            elif 'mail' in source:
                self.__get_mail_data(detail=detail, variables=variables)
            else:
                # setup for custom sql pull #
                detail = detail % self.__handle_replacer(variables)

                # create connection #
                self.sqlib.connect(ctype=source)
                server = self.sqlib[-1]

                # append data #
                data = Table.from_sql(connect=server.c, name=data_name, sql=detail, values=None, parent=server)
                data_list.append(data)
        # except:
        #     onError()
        # finally:
        collect_garbage()
        self.raw.extend(data_list)

    # TODO: 90% sure this method does nothing. Fix this so that it returns like SQL methods.
    def __get_mail_data(self, detail, variables):
        """
        Handles saving mail files via outlook data class.
        :param detail:
        :param variables:
        :return: None
        """
        rules = detail
        path = variables.get('PATH', self.task_path+'\\Data\\')
        save_rule = variables.get('save_rule', '.*')
        data_mode = variables.get('data_mode', 'all')
        OutlookData(rules=rules, path=path, saverule=save_rule, datamode=data_mode)

    def merge(self, master_data_list=None, merge_parameters=None, complete=False, clear_memory=True):
        """
        Merges data via mimic methods or sqlite in memory joins.
        Accepts object of type [(data as index, how, on), ]
        ex) merge(data=[data1,data2], how='inner', on=[(fld0, fld1), (fld2, fld3)])
        sql passed defaults to data1's sql + data2's sql if both exist, else merge sql.
        :param master_data_list: Accepts a list of mimic data_objects.
        :param merge_parameters: Object of type [(data, how, on), ]
            data = [data1, data2, ...] as list indexes
            how = 'inner', 'left', 'outer', 'unite'
        :param complete: If True attempts to merge all data in master_data_list into one file using provided merge
            parameters
        :return: List of mimic data data_objects
        """
        # try:
        if not master_data_list:
            master_data_list = self.raw
        if not merge_parameters:
            merge_parameters = self.merge_parameters
        if master_data_list:
            if merge_parameters:
                ilist = []
                if complete:
                    merge_parameter = merge_parameters[0]
                    i = 0
                    while len(master_data_list) > len(ilist)+1:
                        merge_parameter = ([0, 1+i], merge_parameter[1], merge_parameter[2])
                        data, i0, i1 = self.__merge(data_list=master_data_list, merge_parameters=merge_parameter)
                        master_data_list[i0] = data
                        ilist.append(i1)
                        i += 1
                else:
                    for element in merge_parameters:
                        data, i0, i1 = self.__merge(data_list=master_data_list, merge_parameters=element)
                        master_data_list[i0] = data
                        ilist.append(i1)
                ilist.sort()
                ilist.reverse()
                for i in ilist:
                    del master_data_list[i]
            else:
                broadcast(msg='No merge parameters provided.', clamor=3)
        else:
            broadcast(msg='No data to merge.', clamor=0)
        # except:
        #     onError()
        # finally:

        # clean tables from merge out of memory so there is no conflict with next merge group called #
        if clear_memory:
            self.sqlib.db_clear_memory()
        collect_garbage()
        for i, table in enumerate(master_data_list):
            table.name = 'Data%i' % i
        self.merged = master_data_list

    @staticmethod
    def __merge(data_list, merge_parameters):
        """
        Internal merge function.
        :param data_list: data_list
        :param merge_parameters: merge_parameters
        :return: data WorkingTable, data_list index0, data_list index1
        """
        indices, how, on = merge_parameters
        index0, index1 = indices
        data0, data1 = data_list[index0], data_list[index1]
        data = data0.join(other=data1, how=how, on=on)
        return data, index0, index1

    def process(self, data_list=None):
        """
        Processes task. Writes to template and saves versioned report. Then hides data via com.
        :param data_list: list of mimic data_objects
        :return: None
        """
        # try:
        # setup #
        if not data_list:
            if self.merged:
                data_list = self.merged
            else:
                data_list = self.raw

        # set path #
        self.TASKPATH()

        # call process methods #
        if data_list:
            if self.write_date_data:
                data_list.append(self.get_date_data())
            if self.write_data:
                self.auto_write(data_list=data_list)
            if self.backup_data:  # is True:..?
                self.backup(data_list=data_list)

        # if self.com_type in ['custom']:
        #     self.com_custom()
        # except:
        #     onError()
        # finally:

    def get_date_data(self):
        header = ['start date ty', 'end date ty', 'start date ly', 'end date ly']
        data = [(self.rundate_i, self.rundate_f, self.ly_i, self.ly_f), ]
        date_data = Table(children=data, name='date_data', header=header, note=DATE_LY_SQL)
        return date_data

    def distribute(self):
        """
        Handles file distribution based on __init__'s distribution_parameters unpack.
        :return: None
        """
        # try:
        self.TASKPATH()

        if self.distro_path and any([self.to, self.cc, self.bcc, self.recipients]):
            self.distribute_file()
            self.email(primary=False)
        elif self.distro_path:
            self.distribute_file()
        elif any([self.to, self.cc, self.bcc, self.recipients]):
            self.email(primary=True)

        if self.confirm:
            self.confirmation_email()
        # except:
        #     onError()
        # finally:

    def distribute_file(self):
        if self.date_folder:
            distro_path = self.distro_path+r'\%s' % self.rundate_f.strftime(DATE_FOLDER_FORMAT)
        else:
            distro_path = self.distro_path
        if not os.path.exists(distro_path):
            os.makedirs(distro_path)
        for f in self.files:
            if f:
                if f.strip().endswith('*'):
                    f = ambiguous_extension(f)
                rwc_file(path=distro_path, filename=f, rwc_mode='c+')

    # ## mid level run functions ###
    def backup(self, data_list):
        """
        Sets PATH to master and backs up data in local sqlite database.
        Default index is on run_date and sub_task.
        :param data_list: list of WorkingTable data_objects.
        :return: None
        """

        broadcast(msg='backing up data...', clamor=1)

        # setup #
        if not data_list:
            if self.merged:
                data_list = self.merged
            else:
                data_list = self.raw

        # set path #
        self.ROOTPATH()

        # call backup method on each data object #
        for i, data in enumerate(data_list):
            table_name = self.task_name+'_%i' % i
            self.__backup(data=data, table_name=table_name)
        collect_garbage()
        broadcast(msg='data backed up.', clamor=1)

    def __backup(self, data, table_name=None, overwrite=False):

        # setup #
        old_name = data.name
        if not table_name:
            table_name = self.task_name
        data.name = table_name

        # add run_date and sub_task (if exists) columns #
        added = 0
        data['sub_task'] = [self.sub_task] * len(data[0])
        data.move_to_index(old_index=-1, new_index=0)
        added += 1
        data['run_datetime'] = [self.now] * len(data[0])
        data.move_to_index(old_index=-1, new_index=0)
        added += 1

        # create if not backup db #
        try:
            db = self.sqlib[self.backup_database]
        except KeyError:
            self.sqlib.connect(ctype=self.backup_database)
            db = self.sqlib[-1]

        # write #
        if overwrite:
            try:
                data.db_drop(connect=db.c)
            except Exception:  # TODO: define error
                pass
        data.db_write(connect=db.c)

        # index (guess at indices..? index on run_date and sub_task if self.sub_task?)
        # reindex
        data.db_reindex(connect=db.c)

        for x in range(added):
            del data[0]

        data.name = old_name

    def auto_write(self, data_list, template=None):
        """
        Performs standard report writes into excel data tab for each item in data_list.
        Each item gets its own SQL tab.
        Sets path to TASKPATH().
        """
        broadcast(msg='writing data...', clamor=1)

        # change to task path #
        self.TASKPATH()

        # make data names unique for write #
        real_names = [table.name for table in data_list]
        data_names = ['Data%i' % i for i in range(len(data_list))]

        for table_name, table in zip(data_names, data_list):
            table.name = table_name

        # define template name and clarify ambiguous extension #
        if not template:
            if self.template:
                template = self.template
            else:
                template = 'Template_'+self.task_name+self.ext
        try:
            template = self._get_ext(base_name=template)
        except TypeError:
            template = 'Template_'+self.full_task
            template = self._get_ext(base_name=template)
        ext = template.split('.')[-1]

        # define final save and data names #
        save_name = self.full_task+' '+str(self.rundate_f)+'.'+ext
        data_name = self.full_task+' '+str(self.rundate_f)+' data.xlsx'

        # instantiate writer and execute write #
        w = ExcelWriterV2()
        w.write(data_list=data_list, file_name=data_name)

        # copy using com to desired destination file sheets and sheet locations #
        default_copy(from_file_name=data_name, to_file_name=template, save_file_name=save_name,
                     header=self.write_header, opacity=self.write_opacity, locations=self.write_locations,
                     collapse=self.collapse)

        # collect garbage #
        collect_garbage()

        # reset data object names #
        for table_name, table in zip(real_names, data_list):
            table.name = table_name

        broadcast(msg='data written.', clamor=1)

    @staticmethod
    def _get_ext(base_name):
        result = base_name
        if base_name.endswith('*'):
            result = ambiguous_extension(filename=base_name)
            if result == base_name:
                raise TypeError('Unable to match ambiguous extension in %s.' % base_name)
        return result

    # def com_custom(self):
    #     """
    #     VBA in another sheet with arguments.
    #     :return: None
    #     """
    #     self.ROOTPATH()
    #     collect_garbage()
    #     broadcast(msg='executing VBA...', clamor=3)
    #     broadcast(msg='VBA executed.', clamor=3)
    #     pass

    def email(self, primary=True):
        """ Sets PATH to task and distributes data via Eml class' auto method. """
        self.TASKPATH()
        if self.body_image:
            self.image_to_email()
        broadcast(msg='emailing data...', clamor=1)
        smtp_server, smtp_port = self.get_server('smtp server')
        if primary:
            e = Eml(sender=self.sender, to=self.to, subject=self.subject, message=self.message,
                    files=self.files, smtp_server=smtp_server, smtp_port=smtp_port, cc=self.cc,
                    bcc=self.bcc, password=self.password, path=None, embed=self.embed, embedh=self.embedh,
                    recipients=self.recipients)
        else:
            link_message = '''<a href="%(link)s">%(task)s Link.</a><br>''' % {'link': self.distro_path,
                                                                              'task': self.full_task}
            link_message += self.message
            e = Eml(sender=self.sender, to=self.to, subject=self.subject, message=link_message, files=None,
                    smtp_server=smtp_server, smtp_port=smtp_port, cc=self.cc, bcc=self.bcc, password=self.password,
                    path=None, embed=self.embed, embedh=self.embedh, recipients=self.recipients)
        e.auto()
        collect_garbage()
        if e.files and e.files is not True:
            f = ', '.join(e.files)
        else:
            f = 'email with no attachments'
        r = ', '.join(e.recipients)
        broadcast(msg="""Email Sent: \nTo: %s, \nFrom: %s, \nAttached: %s.""" % (r, self.sender, f), clamor=1)

    def image_to_email(self):
        # # define final name parameters ##
        workbook_name = self.body_image.get('workbook_name', self.full_task+' '+str(self.rundate_f)+self.ext)
        if workbook_name.strip().endswith('*'):
            workbook_name = ambiguous_extension(filename=workbook_name)
        worksheet_name = self.body_image.get('worksheet_name', 1)
        row_start = self.body_image.get('row_start', 1)
        col_start = self.body_image.get('col_start', 1)
        row_end = self.body_image.get('row_end', None)
        col_end = self.body_image.get('col_end', None)
        image_name = self.body_image.get('image_name', u'image.png')
        height = self.body_image.get('height', 380)
        width = self.body_image.get('width', 800)
        # print locals()
        # # save image of range provided ##
        default_range_to_image(
            workbook_name=workbook_name,
            worksheet_name=worksheet_name,
            row_start=row_start,
            col_start=col_start,
            row_end=row_end,
            col_end=col_end,
            image_name=image_name,
            height=height,
            width=width,
        )
        # # set parameters for email ##
        self.embed = image_name

    def confirmation_email(self):
        """
        Sends a confirmation email to user_name's default email address.
        :return: None
        """
        default_email = self.sqlib.get_user_defaults()[2]
        confirm_to = [default_email]
        confirm_from = default_email
        confirm_subject = "Confirming Task Run: %s" % self.full_task
        smtp_server, smtp_port = self.get_server(server_type='smtp')
        confirm_message = """
        <br>%s,
        <br><b>The following are all instance variables at run conclusion:</b>
        """ % self.user_name
        for item in self.__dict__.items():
            if item[0] not in ['data_list', 'merged']:
                if item[0] == 'unexpected_parameters':
                    confirm_message += '<br><b><font color="red">%24s:</font></b> <i>%r</i>' % item
                else:
                    confirm_message += "<br><b>%24s:</b> <i>%r</i>" % item
        broadcast(msg=confirm_message, clamor=8)
        e = Eml(sender=confirm_from, to=confirm_to, subject=confirm_subject,
                message=confirm_message, smtp_server=smtp_server,
                smtp_port=smtp_port, password=self.password)
        e.auto()
        broadcast(msg="Confirmation email sent to %s's default email address." % self.user_name, clamor=3)

    # TODO: finish cleanup methods.
    def close_all(self):
        """
        Sets PATH to master and attempts to close all open database connections.
        Designed to be used in a 'finally' statement.
        """
        self.ROOTPATH()
        self.sqlib.close()
        collect_garbage()

    # TODO: add this to sqlib when it becomes necessary.
    def get_server(self, server_type):
        """
        Gets a server from globals table.
        'smtp server','http proxy','ftp proxy'
        """
        with Path(self.master_path):
            servers = self.backends.get_global_defaults(connect=MASTER)
        smtpserver, smtpport = None, None
        for server in servers:
            if server[1].lower().startswith(server_type):
                smtpserver, smtpport = server[3], server[4]
        return smtpserver, smtpport

    # ## sub-functions ###
    @staticmethod
    def __get_ldow(index):
        """
        counts backwards from tuesday
        tuesday = 6... monday = 0
        :param index: weekday date
        :return: last weekday date
        """
        now = datetime.datetime.now()
        ld = now - datetime.timedelta(index+now.weekday())
        if now - ld > datetime.timedelta(7):
            ld = ld + datetime.timedelta(7)
        return ld

    def __set_datalimit(self):
        """
        Sets data limit for VBA.
        """
        if not self.data_parameters:
            self.data_limit = 0
        elif not self.merge_parameters:
            self.data_limit = len(self.data_parameters)
        elif self.merge_parameters:
            self.data_limit = len(self.data_parameters) - len(self.merge_parameters)
        else:
            self.data_limit = 0

    def __handle_replacer(self, replacer):
        """
        Formats data detail via replacements in variables.
        """
        for key, value in {'rundate_f': self.rundate_f,
                           'ly_f': self.ly_f,
                           'rundate_i': self.rundate_i,
                           'ly_i': self.ly_i}.items():
            replacer[key] = value
            for k, v in replacer.items():
                if v == key:
                    replacer[k] = value
        return replacer


class RptAdv(RptMaster):
    """
    Allows for case statements, full run period rollups, upc subsets, and some other stuff...
    """
    def __init__(self):
        super(RptAdv, self).__init__()


class RptStd(RptMaster):
    """
    Standard Reporting Module.
    Accepts time frames or keywords in addition to standard metrics.
    Time Frames are inherited from RptMaster.
    Metrics include:
        Sales
        Quantity
        Margin
        Cost
        Items
        Transactions
    """
    def __init__(self):
        super(RptStd, self).__init__()