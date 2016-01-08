__author__ = 'drew bailey'
__version__ = 0.1

"""
This file exists solely to pass an object referenced import * from a plugin to an app.
All Plugins will be imported here and relevant information will be passed to app class.
All variables and functions to pass should be in the following class format, functions can have any name

class app_plugin:
    '''
    class containing variables, functions, and function information to pass to parser.
    THIS CLASS IS NOT CALLED
        variables should be those required to initialize target app class
        info dictionary should be named info and have at least three nested keys for each function key
            -rule_index -> call this function to supplement which rule index
            -field_num -> the number of fields to add to the output
            -header_index -> the index positions of the values to add
        functions should be @staticmethod, this class is not called
    '''
    task_name = 'TestParse'
    header = [('pull_end_dt', True),
              ('div_num', True),
              ('comp_name', True),
              ('comp_sto_num', True),
              ('zone_name', True),
              ('sto_num', True),
              ('zone_num', True),
              ('comp_zone_num', True),
              ('weight_each', True),
              ('base_shelf', True),
              ('catg_num', False),
              ('scat_num', False),
              ('catg_desc', False),
              ('alb_ext_rtl', False),
              ('comp_ext_rtl', False),
              ('alb_gross', False),
              ('comp_gross', False),
              ('alb_gper', False),
              ('comp_gper', False),
              ('alb_mix', False),
              ('comp_mix', False),
              ('ind', False),
              ('total', False),
              ('dept_name', False)]
    linerules = ["(?i)^RUN\s+.+ENDING\s+(\d+[-:]\d+[-:]\d+)",
                 "(?i)^(\d+)\s+(\w+)\s+(\d+)\s+(\w+)\s+(\d+)\s.+(\d+)\s.+(\d+)\s+(.+)\s+([B|S]\s+TO\s+[B|S]).*$",
                 "(?i)^ (\d*)\s+(\d*)\s+([\w -]+\w)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s*$"]
    trigger = 20
    data_mode = 'latest'

    def __init__(self):
        pass

    @staticmethod
    def function_one(var):
        pass

    @staticmethod
    def function_two(var):
        pass

    ...

    info = {'function_one':
             {'rule_index': 2,
              'field_num': 2,
              'header_index': [23, 24]}
            'function_two':
                {...}}
"""

from ..data.parser.parser_plugins import parser_cpi

__all__ = ['parser_cpi']
