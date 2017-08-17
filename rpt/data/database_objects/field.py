__author__  = 'drew bailey'
__version__ = 0.1

"""
Combination data container and database container object.
Lowest level database object.
"""

from ..sql import BACKENDS
from .root import Root, check_parent
from ..data_objects.simple_vector import SimpleVector


class Field(SimpleVector, Root):
    """
    MRO (Method Resolution Order): Field -> SimpleVector -> Root -> list -> object
    """

    parent_classes = [None, Root]
    child_class = [object]

    def __init__(self, children=(), name='', parent=None, **kwargs):
        super(Field, self).__init__(children, name=name, name_clean_level=self.name_clean_level, parent=parent, **kwargs)
        self._name = name

    def __repr__(self):
        return super(Field, self).__repr__()[:-1] + ' len ' + str(len(self)) + '>'

    def __setitem__(self, key, value):
        """
        Uses list.__setitem__.
        Overrides Root.__setitem__ since checking child classes slows a large load considerably.
        """
        index = self._key_to_index(key=key)
        list.__setitem__(self, index, value)

    def index(self, value, start=None, stop=None):
        return list.index(self, x=value, i=start, j=stop)

    @check_parent
    def db_drop(self, connect=None, table=None):
        BACKENDS.drop_field(connect=connect, table=table, field=self.name)

    @check_parent
    def db_move(self, connect=None, old=None, new=None):
        NotImplementedError('Method not yet implemented.')

    @check_parent
    def db_rename(self, connect=None, table=None, new_name=None):
        BACKENDS.rename_field(connect=connect, table=table, old=self.name, new=new_name)
