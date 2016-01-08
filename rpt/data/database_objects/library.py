__author__ = 'drew bailey'
__version__ = 0.1

"""
Manages groups of server connections.
"""

from ...config import MASTER_PATH, MASTER_TABLES
from ...util import broadcast, get_date
from ..sql import BACKENDS
from .root import Root
from .server import Server, LOCAL_STRINGS, MEMORY_STRINGS, PYODBC_STRINGS, MASTER_MEMORY_STRINGS, MASTER_STRINGS


class Library(Root):
    """

    """

    parent_classes = [None]
    child_classes = [Server, Root]

    def __init__(self, user_name=None, children=(), name='', parent=None, **kwargs):
        super(Library, self).__init__(children=children, name=name, parent=parent, **kwargs)

        self.user_name = user_name
        self.master_path = kwargs.get('master_path', MASTER_PATH)
        self.path = kwargs.get('path', MASTER_PATH)

        self.mcon, self.mcrs = self.mc = self.__init_db(ctype=MASTER_STRINGS[0], path=self.master_path)
        self.tcon, self.tcrs = self.tc = self.__init_db(MASTER_MEMORY_STRINGS[0], path=self.master_path)
        self.lcon, self.lcrs = self.lc = self.__init_db(LOCAL_STRINGS[0], path=self.path)
        self.econ, self.ecrs = self.ec = (None, None)
        if self.user_name:
            self.econ, self.ecrs = self.ec = self.__init_ext_db()

    def __init_db(self, ctype, path):
        self.append(Server(ctype=ctype, name=ctype, parent=self, path=path))
        return self[-1].c

    def __init_ext_db(self):
        defaults = self.get_user_defaults()
        self.append(Server(ctype=defaults[1], name=PYODBC_STRINGS[0], parent=self))
        return self[-1].c

    def get_user_defaults(self):
        return BACKENDS.get_user_defaults(connect=self.mc, user_name=self.user_name)[0]

    def get_user_connection(self, con_name):
        return BACKENDS.get_user_connection(connect=self.mc, user_name=self.user_name, database=con_name)

    def get_user_connections(self):
        return BACKENDS.get_user_connections(connect=self.mc, user_name=self.user_name)

    def connect(self, ctype, **kwargs):
        """

        :param ctype:
        :param kwargs:
            path
            name
            ...
        :return:
        """
        preloaded = self.get_user_connection(ctype)
        if preloaded:
            self.append(Server(ctype=preloaded[0][1], name=preloaded[0][0], parent=self, **kwargs))
        elif ctype:
            self.append(Server(ctype=ctype, parent=self, **kwargs))

    def db_shard(self, server, to_path=None):
        if not self._is_child(server):
            server = self[server]

        if server.is_lite():
            date = get_date()
            shard_name = '%s %s (SHARD).db' % (str(date), server.name)
            if to_path:
                shard_name = to_path + shard_name
            self.connect(ctype=shard_name)
            shard = self[-1]
            shard.name = server.name+'_shard'

            sql = "".join(line for line in server.con.iterdump())
            shard.con.executescript(sql)
            shard._db_drop_protected()
            server.db_drop_all(protected=True)
        else:
            raise NotImplementedError('Sharding is only implemented for sqlite connections.')

    def db_clear_memory(self):
        self[MASTER_MEMORY_STRINGS[0]].db_drop_all(protected=False)
