__author__  = 'drew bailey'
__version__ = 0.1

"""

"""

from ..__box__ import DIV_0
# from ..data.sql import BACKENDS
from ..loggers import onError, cLog
from ..util import Path, broadcast
import multiprocessing
import traceback
import datetime
import time
import sys
import os


class CronTools(object):
    """
    A toolkit for time related modules.
    Runs off sqlite.
    """
    def __init__(self):
        # self._cronT = CRON_TABLE
        # self._cfields = CRON_FIELDS
        # self._clogT = CRON_LOG_TABLE
        # self._clogfields = CRON_LOG_FIELDS  # to be decided, user, run_dt, error/success

        self.task_name = None
        self.dtat = None
        self.runtype = None
        self.delta = None
        # self.dformatex = transform_string_date(datetime.now())

    @staticmethod
    def counter(iters=60, wait=1):
        """
        Sleeps for 'wait' seconds 'iters' times, counts in shell.
        """
        for i in range(iters):
            sys.stdout.write("\r"+str(i+1)+"  ")
            sys.stdout.flush()
            time.sleep(wait)

    @staticmethod
    def timeout(function, limit, *args):
        """
        Executes a function in a seperate process.
        On completion or after limit seconds the task is terminated and joined.
        A limit value of None results in no timeout condition.
        Additional arguments are passed to the function as tuple from *args.
        """
        p = multiprocessing.Process(target=function, name="Process_p", args=args)
        start = time.time()
        p.start()
        p.join(limit)
        elapsed = time.time()-start
        if elapsed >= limit:
            # print "'%s' is_alive: %s"%(function.__name__,p.is_alive())
            p.terminate()
            time.sleep(1)
            print "'%s' is_alive: %s" % (function.__name__, p.is_alive())
            cond = 'terminated'
        else:
            cond = 'completed'
        print "Function '%s' %s after %s seconds." % (function.__name__, cond, elapsed)
        # print 'isopen %s'%p.is_alive()

    @staticmethod
    def waiter(wait=60):
        """ Waits for 'wait' seconds."""
        start = time.time()
        print 'waiter called.'
        time.sleep(wait)
        elapsed = time.time() - start
        print 'waiter finished: %s.' % elapsed
        # with open('timefile.txt','w') as f:
        #    f.auto_write('%s'%elapsed)

    @staticmethod
    def not_error(path=None):
        if path:
            with Path(new=path):
                with open('noterror.txt', 'w'):
                    pass
                try:
                    time.sleep(3)
                    os.remove('noterror.txt')
                except Exception:
                    pass
        else:
            with open('noterror.txt', 'w'):
                pass
            try:
                time.sleep(3)
                os.remove('noterror.txt')
            except Exception:
                pass

    # ## NOT STATIC BECAUSE OF SUB-FUNCTIONS ###
    def error_file_finder(self, path=None):
        p = multiprocessing.Process(target=self.__eff_sub(), name="Process_p")  # , args=args)
        try:
            while True:
                if path:
                    with Path(new=path):
                        go = self.__err_sub()
                        if go:
                            p.start()
                else:
                    go = self.__err_sub()
                    if go:
                        p.start()
                if go:
                    break
        except Exception:
            onError()
            return -1
        finally:
            p.join()
            time.sleep(1)
            p.terminate()

    @staticmethod
    def __eff_sub():
        try:
            with open('error.txt', 'r') as f:
                err = f.readlines()
            onError(error=err)
        except Exception:
            pass

    @staticmethod
    def __err_sub():
        try:
            with open('error.txt'):
                pass
            return True
        except Exception:
            try:
                with open('noterror.txt'):
                    pass
                return True
            except Exception:
                time.sleep(1)
                return False
        finally:
            try:
                os.remove('noterror.txt')
            except Exception:
                pass
            try:
                os.remove('error.txt')
            except Exception:
                pass


class TimeoutError(Exception):
    """ Exception when task takes longer than expected. """
    pass


class TaskWrapper(object):

    def __init__(self, task='Unknown Task'):
        self.task = task
        self.start = None
        self.start_datetime = None
        self.end = None
        self.end_datetime = None
        self.result = None

    @property
    def elapsed_time(self):
        """

        :return:
        """
        return self.end_datetime - self.start_datetime

    @property
    def elapsed_seconds(self):
        """

        :return:
        """
        return self.end - self.start

    # TODO: LOG ON ENTER
    def __enter__(self):
        """

        :return:
        """
        self.start = time.time()
        self.start_datetime = datetime.datetime.now()
        enter_format = (self.task, self.start_datetime)
        broadcast(msg=DIV_0, clamor=2)
        broadcast(msg="Task '{0}' Beginning {1}.".format(*enter_format), clamor=2)
        broadcast(msg=DIV_0, clamor=2)

    # TODO: LOG ON EXIT
    def __exit__(self, exception_type, exception_value, trace):
        """

        :param exception_type:
        :param exception_value:
        :param trace:
        :return:
        """
        self.end = time.time()
        self.end_datetime = datetime.datetime.now()
        exit_format = (self.task, self.end_datetime, self.elapsed_time, self.elapsed_seconds)

        broadcast(msg=DIV_0, clamor=2)
        if exception_type:
            broadcast(msg="Task '{0}' Failed {1}.\n{2} (~{3:,.2f} seconds) elapsed.".format(*exit_format), clamor=2)
        else:
            broadcast(msg="Task '{0}' Completed {1}.\n{2} (~{3:,.2f} seconds) elapsed.".format(*exit_format), clamor=2)
        broadcast(msg=DIV_0, clamor=2)
        if exception_type:
            # TODO: print this trace to log.
            # traceback.print_exception(exception_type, exception_value, trace)
            pass
