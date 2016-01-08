__author__ = 'drew bailey'
__version__ = 0.2

"""
Runs tasks at scheduled times and updates task and task logs tables.
"""

from ..config import CRON_TABLE, MASTER_PATH, CRON_FIELDS
from ..util import transform_string_date, Path
from ..loggers import cLog
from .crontools import CronTools, TaskWrapper
from ..data.sql import MASTER, BACKENDS
from ..data import explicit_py_type, Table
import datetime
import time


# TODO: allow setup new task from Cron.
class Cron(object):
    """
    A class devoted to running scheduled tasks. Reads a cron table generated
    by the CronSetup class and executes files in task folders.
    """
    def __init__(self):
        self.master_path = MASTER_PATH
        self.task_path = None
        self.check = None
        self.date_format_example = transform_string_date(datetime.datetime.now())

    def get_next_task(self):
        """
        Returns correct task to run or None.
        :return:
        """
        self.check = datetime.datetime.now()
        all_tasks = self.all().rows()
        due_tasks = []
        for task in all_tasks:
            run_alpha = task[self.index('next_run')]
            if isinstance(run_alpha, basestring):
                run_alpha = transform_string_date(run_alpha)
            if not task[self.index('last_run')]:
                run_omega = datetime.datetime(datetime.MAXYEAR, 1, 1)
            else:
                run_omega, py_type = explicit_py_type(obj=task[self.index('last_run')], parse_dates=True)
            if run_omega > self.check > run_alpha:
                due_tasks.append(task)
        if due_tasks:
            priorities = [t[self.index('priority')] for t in due_tasks]
            return due_tasks[priorities.index(max(priorities))]
        return None

    def run_task(self, task_name=None, update=False):
        """
        One time task run. Runs task_name if provided, else runs next scheduled task.
        :param task_name: task_name of task to run.
        :param update: if True updates Cron table.
        :return: None
        """
        self.check = datetime.datetime.now()
        # get correct task to run
        if task_name and not update:
            task = [None] * len(CRON_FIELDS)
            task[self.index('task_name')] = task_name
        elif task_name:
            task = self.get_task(task_name=task_name)
            if not task.rows():
                raise ValueError("Task '%s' not found in table %s." % (task_name, CRON_TABLE))
            task = task.rows()[0]
        else:
            task = self.get_next_task()
            if not task:
                return
            task_name = task[self.index('task_name')]
        # init wrapper
        with TaskWrapper(task_name):
            # run task
            self.__run_task(task=task)
            # update task as run in cron table
            if update:
                self.__update(task=task)

    def __run_task(self, task):
        """
        Imports a variable 'task'.py and logs finish or error.
        debug = True allows program to stop and displays full traceback.
        """
        # start = time.time()
        # CronLogger(elapsed=0, PATH=self.MASTER_PATH).clog.info('Task %s started.' % task)
        # try:
        task_name = task[self.index('task_name')]
        with Path(task_name):
            x = __import__(task_name)
            x.main()
        # elapsed = time.time() - start
        # CronLogger(elapsed=elapsed).clog.info('Task %s finished.' % task_name)
        # except:
        #     onError()
        #     elapsed = time() - start
        #     CronLogger(elapsed=elapsed, PATH=self.MASTER_PATH).clog.info('Task %s finished.') % task

        # ct.not_error(PATH=None)

    def __update(self, task):
        last_run = task[self.index('last_run')]
        if not last_run:
            last_run = self.check
        else:
            last_run = transform_string_date(last_run)
        delta = datetime.timedelta(seconds=task[self.index('delta')])
        next_run = self._get_next_run(last_run=last_run, delta=delta)
        unusual_runs = self.__parse_flagged(task=task)
        if unusual_runs:
            next_run = self._check_flagged_runs(
                next_run=next_run,
                unusual_runs=unusual_runs,
                task_name=task[self.index('task_name')],
                user_name=task[self.index('user_name')])
        self._edit_next_run(task=task, next_run=next_run)
        self._insert_last_run(task=task, last_run=self.check)

    def __parse_flagged(self, task):
        try:
            return eval(task[self.index('flagged_runs')])
        except Exception:  # TODO: define exception.
            return None

    def _get_next_run(self, last_run, delta):  # shared
        """
        Finds the first run to fall in the future.
        :param last_run: datetime.datetime object representing task's last run date and time.
        :param delta: datetime.timedelta object representing time between task runs.
        :return datetime.datetime object representing next run time.
        """
        check = self.check
        # next_run = transform_string_date(last_run)
        next_run = last_run
        while next_run < check:
            if delta:
                next_run += delta
            else:
                next_year = next_run.year
                next_month = next_run.month
                if next_month == 12:
                    next_month = 1
                    next_year += 1
                else:
                    next_month += 1
                next_run = datetime.datetime(next_year, next_month, next_run.day, next_run.hour, next_run.minute)
            check = datetime.datetime.now()
        return next_run

    # def _get_run_end(self, task):
    #     """ gets cron task's run end date """
    #     self.sqlib.local.select(table_name=CRON_TABLE, fields=['run_end_dt'], where=[('task_name', task)])

    def _edit_next_run(self, task, next_run):
        """
        Edits the next run time in the database.
        """
        BACKENDS.alter_values(connect=MASTER,
                              table=CRON_TABLE,
                              where=[('TASK_NAME', task[self.index('task_name')])],
                              sets=[('NEXT_RUN', next_run)])

    def _insert_last_run(self, task, last_run):
        """
        Inputs last run into cron table.
        """
        BACKENDS.alter_values(connect=MASTER,
                              table=CRON_TABLE,
                              where=[('TASK_NAME', task[self.index('task_name')])],
                              sets=[('LAST_RUN', last_run)])
        
    def begin(self, infinite=False):
        """
        Loops through tasks. If infinite = True will not stop unless critical error occurs.
        """
        while True:
            self.run_task(update=True)
            if not infinite:
                break
            CronTools.counter(5)

    def _check_flagged_runs(self, next_run, unusual_runs, task_name, user_name):
        """
        Checks unusual (flagged) runs and sets next run to a flagged run if necessary. Removes runs from list if they
        are in the past, sorts remaining runs, sends to writer.
        :param next_run: Next run value.
        :param unusual_runs: Current flagged runs list.
        :param task_name: Task_name in table.
        :param user_name: User_name in table.
        :return: next_run after check
        """
        unusual_runs = [run for run in unusual_runs if transform_string_date(run) > self.check]
        unusual_runs.sort()
        for run_str in unusual_runs:
            run_dtat = transform_string_date(dtat=run_str)
            if run_dtat < next_run:
                next_run = run_dtat
                break
        if unusual_runs:
            BACKENDS.alter_values(connect=MASTER,
                                  table=CRON_TABLE,
                                  where=[('TASK_NAME', task_name), ('USER_NAME', user_name)],
                                  sets=[('FLAGGED_RUNS', unusual_runs)])
        return next_run

    @staticmethod
    def index(field_name):
        return CRON_FIELDS.index(field_name.upper())

    def check(self):
        pass

    # TODO: delay task run by delta (for error..? or missing data condition?) how to handle this without altering schedule?
    def delay(self, delta):
        pass

    @staticmethod
    def get_task(task_name=None, cron_id=None, user_name=None, **kwargs):
        """
        Returns a Table of all tasks with the provided task name, task_name and user_name, or cron_id (no case compared).
        :return: Table of tasks with given name.
        """
        where = []
        if task_name:
            where.append(('TASK_NAME', task_name))
        if cron_id:
            where.append(('CRON_ID', cron_id))
        if user_name:
            where.append(('USER_NAME', user_name))
        for key, value in kwargs.items():
            where.append((key.upper(), value))
        result = Table.from_select(connect=MASTER, table_name=CRON_TABLE, where=where, no_case=True)
        return result

    @staticmethod
    def all():
        """
        Returns information about all tasks.
        :return: Table of all scheduled tasks.
        """
        result = Table.from_select(connect=MASTER, table_name=CRON_TABLE)
        return result

    def today(self):
        """
        Returns information about tasks still scheduled to run today.
        :return: Table of scheduled to run this today.
        """
        next_run_index = self.index('next_run')
        limit = datetime.datetime.today() + datetime.timedelta(days=1)
        result = self.all()
        result.drop_rows_where(conditions=[(next_run_index, '>= %r' % limit)])
        return result

    # TODO: include optional end of week day/day index?
    def this_week(self):
        """
        Returns a Table of all tasks still scheduled to run within the next seven days.
        :return: Table of scheduled to run this week.
        """
        next_run_index = self.index('next_run')
        limit = datetime.datetime.today() + datetime.timedelta(weeks=1)
        result = self.all()
        result.drop_rows_where(conditions=[(next_run_index, '> %r' % limit)])
        return result

    def this_month(self):
        """
        Returns a Table of all tasks still scheduled to run this calendar month.
        :return: Table of scheduled to run in the calendar month.
        """
        next_run_index = self.index('next_run')
        limit_ = datetime.datetime.today() + datetime.timedelta(weeks=4)
        limit = datetime.datetime(year=limit_.year, month=limit_.month, day=1)
        result = self.all()
        result.drop_rows_where(conditions=[(next_run_index, '>= %r' % limit)])
        return result

    @staticmethod
    def daily():
        """
        Returns a Table of all tasks scheduled to run daily.
        :return: Table of daily tasks.
        """
        delta = datetime.timedelta(days=1).total_seconds()
        result = Table.from_select(connect=MASTER, table_name=CRON_TABLE, where=[('DELTA', delta), ])
        return result

    @staticmethod
    def weekly():
        """
        Returns a Table of all tasks scheduled to run weekly.
        :return: Table of weekly tasks.
        """
        delta = datetime.timedelta(weeks=1).total_seconds()
        result = Table.from_select(connect=MASTER, table_name=CRON_TABLE, where=[('DELTA', delta), ])
        return result

    def monthly(self):
        """
        Returns a Table of all tasks scheduled to run monthly.
        :return: Table of monthly tasks.
        """
        delta_index = self.index('delta')
        result = self.all()
        result.drop_rows_where(conditions=[(delta_index, '!= %r' % None)])
        return result

    def other(self):
        """
        Returns a Table of all tasks scheduled to run not strictly daily, weekly, or monthly.
        :return: Table of other tasks.
        """
        delta_index = self.index('delta')
        delta_day = datetime.timedelta(days=1).total_seconds()
        delta_week = datetime.timedelta(weeks=1).total_seconds()
        result = self.all()
        result.drop_rows_where(conditions=[(delta_index, '== %r' % None)])
        result.drop_rows_where(conditions=[(delta_index, '== %r' % delta_day)])
        result.drop_rows_where(conditions=[(delta_index, '== %r' % delta_week)])
        return result

    def unusual(self):
        """
        Returns a Table of all tasks scheduled to run not strictly daily, weekly, or monthly.
        :return: Table of other tasks.
        """
        flagged_index = self.index('flagged_runs')
        result = self.all()
        result.drop_rows_where(conditions=[(flagged_index, '== %r' % None)])
        return result

    def ended(self):
        """
        Returns a Table of all tasks that are past their last run date. (Though it is possible that they have not
         completed their final run).
        :return: Table of tasks with RUN_END_DATEs before now.
        """
        end_index = self.index('last_run')
        limit = datetime.datetime.now()
        result = self.all()
        result.drop_rows_where(conditions=[(end_index, '== %r' % None)])
        result.drop_rows_where(conditions=[(end_index, '>= %r' % limit)])
        return result

# c = Cron()
# c.check = datetime.now()
