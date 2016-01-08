__author__  = 'drew bailey'
__version__ = 0.1

"""
Gets data from files.
"""

# combines parsers of different simple_type, not complete.


class FileData(object):
    """
    Combines parsers and outputs an object similar to a fetchall() object.
    Plugs into sqlib via sql_e package where data will be handled like
    query data.
    """
    def __init__(self):
        # file source full PATH
        self.target = None
        # parsing rules program...?
        self.ruleset = None
