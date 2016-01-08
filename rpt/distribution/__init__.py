__author__  = 'drew bailey'
__version__ = 1.1

"""
Distribution methods __init__.
All distribution packages designed to have similar user facing inputs and outputs. There will be however substantial
differences in necessary inputs from the user based on distribution method.
Functions off input dictionary distribution_parameters containing information required for each distribution type.
"""

from .filerwc import rwc_file
from .eml import Eml

__all__ = ['__author__',
           '__version__',
           'Eml',
           'rwc_file']