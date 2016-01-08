__author__  = 'drew bailey'
__version__ = 1.0

"""

"""

from .note import Note


class User(str):
    def __init__(self, name):
        super(User, self).__init__(name)
        self.name = name
        self.password = None
        self.note = Note()
        self.other = None
