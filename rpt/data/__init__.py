__author__ = 'drew bailey'
__version__ = 0.84

"""
Accepts data_parameters lists from users or master level function and returns a WorkingTable containing information from given
source. All inputs and outputs should look similar to users.
    IN  ->  [(source), (detail), (detail variables or replacements)]
        SOURCE      ->  Data source, parser, SQL database, net or web data, etc...
        DETAIL      ->  What to get from the source. SQL code to pass, parsing rules, etc...
        VARIABLES   ->  Additional variables or replacements related to getting data. Injections for SQL code, etc...
    OUT ->  WorkingTable object with data. All appropriate properties filled in.
This module should boil sources and merging down to similar user facing functions.
"""

from .database_objects import (Library, Server, Database, Table, Index, Field)
from .data_objects import (ExcelWriterV2, ExcelReader, explicit_py_type)
from .sql import SQLBackends, BACKENDS, MASTER_MEMORY, MASTER
from .outlookdata import OutlookData
from .exceldata import ExcelData
from .filedata import FileData
from .netdata import NetData
from .parser import Parser


__all__ = ['__author__',
           '__version__',
           'Field',
           'Index',
           'Table',
           'Database',
           'Server',
           'Library',
           'ExcelWriterV2',
           'ExcelReader',
           'explicit_py_type',
           'OutlookData',
           'ExcelData',
           'FileData',
           'NetData',
           'SQLBackends',
           'Parser',
           'BACKENDS',
           'MASTER_MEMORY',
           'MASTER',
           ]
