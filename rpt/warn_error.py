__author__  = 'drew bailey'
__version__ = 0.1

"""
Package specific errors and warnings.
"""

import warnings


### General Data ###

class LoadError(Exception):
    pass


### WorkingTable ###

class InvalidKeyError(Exception):
    pass


class MatrixDimensionError(Exception):
    pass


### Vector Operations ###

class ACIDAtomicityError(Exception):
    pass


class ACIDConsistencyError(Exception):
    pass


class ACIDIsolationError(Exception):
    pass


class ACIDDurabilityError(Exception):
    pass


class ACIDAtomicityWarning(RuntimeWarning, Exception):
    pass


class ACIDConsistencyWarning(RuntimeWarning, Exception):
    pass


class ACIDIsolationWarning(RuntimeWarning, Exception):
    pass


class ACIDDurabilityWarning(RuntimeWarning, Exception):
    pass


# come back to this
def ACIDException(msg='', fail_type='C', error=False):

    if fail_type.startswith('c'):
        warnings.warn(msg, ACIDAtomicityWarning)
