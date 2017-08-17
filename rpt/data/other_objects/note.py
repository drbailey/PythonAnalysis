__author__  = 'drew bailey'
__version__ = 1.0

"""
Note class holds SQL and other user and package strings in an organized way that won't allow flooding a shell or a
 file output. All notes share settings.
"""


DEFAULT_MAX_PRINT_CHAR = 2000


class Note(basestring):

    # settings as list will allow for changes across all note objects
    _settings = {
        'max_print_char': DEFAULT_MAX_PRINT_CHAR,
         }

    def __init__(self):
        super(Note, self).__init__()

    def __str__(self):
        string = self[:self.max_print_char]
        basestring.__str__(string)

    @property
    def max_print_char(self):
        return self._settings['max_print_char']

    @max_print_char.setter
    def max_print_char(self, value):
        self._settings.update({'max_print_char': value})

    @max_print_char.deleter
    def max_print_char(self):
        self._settings.update({'max_print_char': DEFAULT_MAX_PRINT_CHAR})

   # TODO: for join sql once this object is used.
    def __generate_joined_sql(self, primary, other, sql):
        """
        Generates a more verbose SQL statement for informational and write purposes. Should allow for replication
            through traceback. This SQL statement will not execute.
        """
        new_sql = """

%s: <
%s>

%s: <
%s>""" % (self.name, self.sql, other.name, other.sql)
        broadcast(msg=sql, clamor=9)
        return sql + new_sql