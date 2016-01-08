__author__ = 'drew bailey'
__version__ = 2.0

"""
Logging handlers for package SQLite3 logging.
All package logging style objects reside here.
"""

from ..__box__ import MASTER_PATH, LOG_TABLE, LOG_FIELDS, LOG_TYPES, CRON_LOG_TABLE, CRON_LOG_FIELDS, CRON_LOG_TYPES
from ..util import Path
from ..data.sql import BACKENDS, MASTER
import datetime
import logging
import sqlite3


class SQLiteHandler(logging.Handler):
    """
    Primary SQLite3 logging Handler root.
    Other SQLite handlers should inherit from this class.
    """

    if_not = True
    converter = {}
    fields = []

    def __init__(self, connect, table, fields, data_types, name=None, path=None, level=logging.NOTSET):
        # instantiate parents
        super(SQLiteHandler, self).__init__(level=level)
        # set user inputs
        self.connect = connect
        self.table = table
        if not self.fields:
            self.fields = fields
        self.types = data_types
        self.path = path
        # invokes property .name and sets ._name var
        if name:
            self.name = name
        # hold values last emit..?
        self.values = {}
        # create log table if it does not exist
        self.__create_log_table()

    def __create_log_table(self):
        """
        Creates log table. If if_not is True (default) it will not overwrite existing log table.
        """
        with Path(self.path):
            BACKENDS.create_table(connect=self.connect, table=self.table, fields=self.fields,
                                  data_types=self.types, if_not=self.if_not)

    def emit(self, record):
        """

        """
        # self.format(record)
        # update record dict
        self.update_record(record=record)
        # timestamp to datetime
        record.created = datetime.datetime.fromtimestamp(record.created)
        # handle error text
        if record.exc_info:
            record.exc_text = logging._defaultFormatter.formatException(record.exc_info)
        else:
            record.exc_text = ""

        values = self.get_values(record=record)

        try:
            # do insert
            with Path(self.path):
                BACKENDS.insert_rows(connect=self.connect, table=self.table, fields=self.fields[1:], rows=[values[1:]])
        except sqlite3.OperationalError:
            try:
                with Path(self.path):
                    BACKENDS.insert_rows(connect=self.connect, table=self.table, fields=self.fields, rows=[values])
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                self.handleError(record)
        except (KeyboardInterrupt, SystemExit):
            # handle primary exit errors
            raise
        except:
            # handle any other error
            self.handleError(record)
        finally:
            # this does nothing right now, implement if necessary.
            self.flush()

    # def handleError(self, record):
    #     pass

    def update_record(self, record, converter=None):
        if not converter:
            converter = self.converter
        updater = {}
        for field in converter:
            if not record.__dict__.get(field):
                converter.get(field)
                value = record.__dict__.get(converter.get(field))
                updater.update({field: value})
        record.__dict__.update(updater)

    def get_values(self, record, fields=None):
        if not fields:
            fields = self.fields
        values = []
        for field in fields:
            values.append(record.__dict__.get(field))
        return values


class RealHandler(SQLiteHandler):
    """
    Standard package logger.
    Records general messages
    """
    converter = {
        'LOG_ID': None,
        'CREATED': 'created',
        'NAME': 'name',
        'LOG_LEVEL': 'levelno',
        'LOG_LEVEL_NAME': 'levelname',
        'MESSAGE': 'msg',
        'ARGS': 'args',
        'MODULE': 'module',
        'FUNC_NAME': 'funcName',
        'PROCESS': 'process',
        'PROCESS_NAME': 'processName',
        'THREAD': 'thread',
        'THREAD_NAME': 'threadName',
        'EXCEPTION': 'exc_text',
        'LINE_NO': 'lineno',
        }
    fields = LOG_FIELDS

    def __init__(self, connect=MASTER, path=MASTER_PATH, level=logging.DEBUG):
        super(RealHandler, self).__init__(connect=connect, table=LOG_TABLE, fields=self.fields,
                                          data_types=LOG_TYPES, name='RealHandler', path=path, level=level)


class CronHandler(SQLiteHandler):
    """
    Cron and CronTools logger.
    Records task run information.
    """
    converter = {
        'CRON_LOG_ID': None,
        'CRON_ID': '',
        'CREATED': 'created',
        'NAME': 'name',
        'MESSAGE': 'msg',
        'ELAPSED': '',
        'RESULT': '',
        'PROCESS': 'process',
        'PROCESS_NAME': 'processName',
        'THREAD': 'thread',
        'THREAD_NAME': 'threadName',
        'EXCEPTION': '',
        'LINE_NO': 'lineno',
        }
    fields = CRON_LOG_FIELDS

    def __init__(self, connect=MASTER, path=MASTER_PATH, level=logging.DEBUG):
        super(CronHandler, self).__init__(connect=connect, table=CRON_LOG_TABLE, fields=CRON_LOG_FIELDS,
                                          data_types=CRON_LOG_TYPES, name='CronHandler', path=path, level=level)


# setup Loggers #
# rLog = logging.getLogger('RealLogger')
# rLog.setLevel(logging.DEBUG)
# rHandler = RealHandler()
# rLog.addHandler(rHandler)
#
# cLog = logging.getLogger('CronLogger')
# cLog.setLevel(logging.DEBUG)
# cHandler = CronHandler()
# cLog.addHandler(cHandler)

# use:
# cLog.info(msg=msg_text, extra={'arg_1': val_1, 'arg_2': val_2, ...})
# same for rLog

"""
Some notes on building these objects using the default Python logging package:

handlers must...
    - set handler name (handler.name = ...) (name is property and sets logging._handlerList)
    - set level (set low and let Logger.level manage this, annoying to have two levels actively involved)
    - set formatter (not necessary, but would conform to standard)
    - set filter (... if necessary?)
    - lock (auto from parent __init__)
    - set handleError (would be nice to log to a text file via a basic handler)
    - emit record
    - flush (does nothing in this version, does it in 3.x? I'm not streaming so don't worry about for now.)
    - unlock (where to use..? Parent has no uses of Handler.release() that I can find.)

loggers must...
    - set self
    - set level
    - set handler
    - add handler

notes:
    - there is a global handler list already at logging._handlerList (preferred method of accessing?)
        > use logging.getLogger([name])
        > getLogger returns same instance of named handler via weakref. logger is created if it doesn't exist.
        > _handlerList contains only weakref objects to handlers. if this is the only ref the object will be gc'd.
    - when is it appropriate to close handlers? on package exit..? on library exit..?
    - if loggers are library specific than multiple instances would be allowed to access the same tables which could lead to issues.
        > (wait and try again solves most..?)
        > multiple access not granted to cross thread requests
        > for now let package init loggers + handlers
    - all "log this" functions called by user (logger.warn, logger.info, logger.error, ...) call Logger._log()
        > _log expects
        > _log creates a record based on inputs
    - both the Logger and the Handler level must be below the threshold of the type of log sent.
        > ex) if info both Logger and Handler must be of level or lower than level 20 for Handler.emit (from Logger._log) to be called.

"""