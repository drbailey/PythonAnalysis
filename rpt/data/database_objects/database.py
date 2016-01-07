__author__	= 'drew bailey'
__version__	= 0.1

"""

"""

from ..sql import BACKENDS
from .root import Root, check_parent
from .table import Table


class Database(Root):
    """

    """

    parent_classes = [None, Root]
    child_classes = [Table]

    def __init__(self, children=(), name='', parent=None, **kwargs):
        super(Database, self).__init__(children=children, name=name, parent=parent, **kwargs)
