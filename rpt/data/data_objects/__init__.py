__author__ = 'drew bailey'
__version__ = 4.0

"""
Package data object classes and related functions.
"""

from .excel_objects import ExcelWriterV2, ExcelReader
from .simple_types import explicit_py_type
from .simple_vector import SimpleVector

__all__ = ['__author__',
           '__version__',
           'ExcelWriterV2',
           'ExcelReader',
           'explicit_py_type',
           'simple_vector',
           ]