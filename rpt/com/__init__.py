__author__ = 'drew bailey'
__version__ = 0.1

"""
__init__ for com package.
Look into rewriting to run without com.
"""

from .comexcel import ComExcel
from .comoutlook import ComOutlook
from .comappmethods import *


__all__ = ['__author__',
           '__version__',
           'ComExcel',
           'ComOutlook',
           'hide_sheets',
           'encrypt_formula',
           'decrypt_formula',
           'copy_range']
