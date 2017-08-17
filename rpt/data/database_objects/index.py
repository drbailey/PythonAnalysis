__author__  = 'drew bailey'
__version__ = 0.1


"""
A table index.
"""


from ..sql import BACKENDS
from .root import Root, check_parent
from .field import Field


class Index(Root):
    """
    A table index. Children are Field objects.
    """

    parent_classes = [None, Root]
    child_classes = [Field]

    def __init__(self, children=(), name='', primary=False, parent=None):
        super(Index, self).__init__(children, name=name, parent=parent)
        self.primary = primary

    @check_parent
    def db_create(self, connect=None, table_name=None):
        """
        Apply index.
        """
        if not table_name:
            table_name = self.parent.name
        if not table_name:
            LookupError('Please specify table.')
        BACKENDS.create_index(connect=connect, index=self.name, table=table_name,
                              fields=[child.name for child in self], unique=self.primary)

    # TODO: specify table, there can be multiple indices with the same name in a database?
    @check_parent
    def db_drop(self, connect=None, table_name=None):
        """
        Removes this index name from database.
        """
        BACKENDS.drop_index(connect=connect, index_name=self.name)

    @check_parent
    def db_update(self, connect=None, table_name=None):
        """
        Drops ind
        :param connect:
        :return:
        """
        self.db_drop(connect=connect, table=table_name)
        self.db_create(connect=connect, table=table_name)
