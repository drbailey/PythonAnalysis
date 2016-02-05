__author__ = 'drew bailey'
__version__ = 0.1

"""
Test methods for containing library.
To add a new test file:
    1. Add test .py file to an importable directory.
    2. Include an import * to get_test_functions function in this file.
    3. Make sure to import only tests from each file.
NOTE:
    - Every individual test function should create its own environment without relying on
        outside imports.
"""
from .test_primary import *
from .test_data import *

def get_test_functions():
    return [value for key, value in globals().items() if hasattr(value, '__call__')]
    
functions = get_test_functions()

__all__ = [
    'test_all',
    ]

if functions:
    __all__ += functions


def test_all():
    import warn
    if functions:
        for obj in functions:
            obj()
            print obj.__name__
