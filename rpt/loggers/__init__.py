__author__  = 'drew bailey'
__version__ = 1.1

"""
Logging handler and predefined error functions __init__.
Package loggers behave similarly to builtin loggers and pull setup parameters from config. Predefined errors should
use appropriate loggers but look the same to users.
ex)
    onError(error, logger)
"""

from ..util import broadcast
from .logger import RealHandler, CronHandler, SQLiteHandler
import logging
import sys
import os

__all__ = ['__author__',
           '__version__',
           'RealHandler',
           'CronHandler',
           'onError',
           'rLog',
           'cLog',
           ]


rLog = logging.getLogger('RealLogger')
rLog.setLevel(logging.DEBUG)
rHandler = RealHandler()
rLog.addHandler(rHandler)

cLog = logging.getLogger('CronLogger')
cLog.setLevel(logging.DEBUG)
cHandler = CronHandler()
cLog.addHandler(cHandler)


# ERRORS AND FUNCTIONS #
def onError(error=None, logger=None):
    """
    Desired error printing method for entire package.
    :param logger: accepts a logger to assign error to. If none defaults to package RealLogger default.
    :return: None
    """
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    broadcast(msg='Error: %s, \n in %s, \n line %s', clamor=1)
    if logger and error:
        logger.error(error)
    elif logger:
        logger.error('Error: %s, in %s, line %s' % (exc_type, fname, exc_tb.tb_lineno))
    else:
        if error:
            rLog.error(error)
        else:
            rLog.error('Error: %s, in %s, line %s' % (exc_type, fname, exc_tb.tb_lineno))
