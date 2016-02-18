__author__  = 'drew bailey'
__version__ = 0.1

"""

"""


from ...util import clean, broadcast
from ...config import (
    DIV_0,
    CHILD_RESERVED_ERROR,
    CHILD_TYPE_ERROR,
    CHILD_KEY_ERROR,
    PARENT_TYPE_ERROR,
    MIN_PRINT_LENGTH,
    MAX_PRINT_LENGTH,
    MAX_PRINT_COLUMNS,
    MAX_PRINT_ROWS,
    )
from ..sql import BACKENDS
import copy
import sqlite3
import pyodbc


# TODO: volume attribute for broadcast?
class Root(list):
    """
    A root object for all database objects.
    Contains common variables, methods, and objects.
    Any list method that accepts index now accepts a key, that can be either the child name associated or the integer
        index. Any list method accepting a value remains unchanged.
    """

    name_clean_level = 2
    _reserved = None

    # unless otherwise specified root objects allow membership from any class #
    parent_classes = [object, None]
    child_classes = [object]

    min_print_length = MIN_PRINT_LENGTH
    max_print_length = MAX_PRINT_LENGTH
    max_print_columns = MAX_PRINT_COLUMNS
    max_print_rows = MAX_PRINT_ROWS

    database_explicit = True  # sets explicit naming in backends objects

    def __init__(self, children=(), name='', **kwargs):
        self._reserved = []
        self._name = name
        self._parent = kwargs.get('parent', None)
        self.note = kwargs.get('note', '')
        note_extra = kwargs.get('note_extra', None)

        super(Root, self).__init__()
        self._reserved.extend(self.__dict__)  # extends reserved names by list of keys
        if children:
            self.extend(children)

        if not self._name:
            self.name = self._get_default_name(self.__class__)
            # self.name = self.__get_default_name()

        # load note
        try:
            self._load_note(other=children, extra=note_extra)
        except AttributeError:
            pass

        # print self._name+' initialized. (Msg from Root.__init__)'
        self.backends = None
        backends = kwargs.get('backends', None)
        if backends:
            self.backends = backends()
        elif self.parent:
            self.backends = self.parent.backends
        else:
            self.backends = BACKENDS

        self.backends.explicit_naming = self.database_explicit

    @staticmethod
    def _get_default_name(cls):
        return '_'+cls.__name__.lower()+'_'
    # def __get_default_name(self):
    #     return '_'+self.__class__.__name__.lower()+'_'

    def __getitem__(self, key):
        """
        Key can be either a member name or an index.
        """
        index = self._key_to_index(key=key)
        return list.__getitem__(self, index)

    # def __getslice__(self, i, j):
    #     i = self.__key_to_index(i)
    #     j = self.__key_to_index(j)
    #     return list.__getslice__(self, i, j)

    def __setitem__(self, key, value):
        if self._is_child(child=value):
            try:
                index = self._key_to_index(key=key)
                list.__setitem__(self, index, value)
                # if index is not None:  # can index be None?
                #     if index in range(-len(self), len(self)):
                #         list.__setitem__(self, index, value)
                #     elif index < -len(self):
                #         list.insert(self, 0, value)
                #     elif index >= len(self):
                #         list.append(self, value)
                #     else:
                #         raise IndexError('Unknown or unexpected index entry.')
            except KeyError:
                if key in self._reserved:
                    raise KeyError(CHILD_RESERVED_ERROR % key)
                else:
                    list.append(self, value)
                    self[-1].name = key
            except IndexError:
                list.insert(self, key, value)

            # self.__set_name(key, value)
        else:
            try:
                value = self.child_classes[0](children=value)
                self.__setitem__(key, value)
            except:
                raise TypeError(CHILD_TYPE_ERROR %
                                (value.__class__.__name__, str([x.__name__ for x in self.child_classes])))

    # TODO: is this method necessary?
    def __set_name(self, name, other):
        """
        Names objects as they are added if they have a .name attribute.
        :param name:
        :param other:
        :return:
        """
        if hasattr(other, 'name'):
            if name:
                other.name = name
            elif not other.name:
                other.name = self._get_default_name(other.__class__)

    def __getattribute__(self, item):
        try:
            return object.__getattribute__(self, item)
        except (AttributeError, SyntaxError):
            try:
                return self[item]
            except KeyError, err:
                raise AttributeError(err)

    def __setattr__(self, key, value):
        """
        Allows for referencing pre-existing members with .item_key notation.
        CANNOT SET A NEW ITEM WITH __setattr__, use __setitem__ method (self[item_key] = new_child).
        If a member is incorrectly referenced an attribute will be created.
        :param key: attribute name or pre-existing member name.
        :param value: value to set referenced object to.
        :return: None
        """
        # pass goes to standard object setattr method.
        if not self._reserved:
            # allows for standard load during __init__.
            pass
        elif self.__is_reserved(key):
            # allows for alteration of reserved names. Warn user if called directly?
            pass
        else:
            try:
                # captures member objects using set/get item methods. If not member will error inside setitem method.
                self[key]  # will error if object doesn't exist.
                self[key] = value
                return
            except (AttributeError, TypeError, KeyError):
                # cannot create a new item with setattr;
                # will reference an attribute if member with that name doesn't exist.
                pass
        object.__setattr__(self, key, value)
        return

    # TODO: slice (get, set, del) methods need to be re-evaluated. for now base list methods used, this can bypass load methods.
    # def __setslice__(self, i, j, y):
    #     i = self.__key_to_index(i)
    #     j = self.__key_to_index(j)
    #     for i, item in zip(range(i, j), y):
    #         self[i] = item

    def __delitem__(self, key):
        index = self._key_to_index(key=key)
        if isinstance(index, int):
            return list.__delitem__(self, index)
        raise KeyError('Unknown or unexpected key.')

    def __delattr__(self, key):
        if key not in dir(self):
            return self.__delitem__(key=key)
        if self.__is_reserved(key):
            self.__dict__[key] = None
        return object.__delattr__(self, key)

    # TODO: I don't work as intended. Copy is a very slow method (deepcopy goes down the rabbit hole).
    def __add__(self, other):
        base = self.copy()
        if isinstance(other, self.__class__):
            base.extend(other)
            return base
        elif self._is_child(other):
            base.append(other)
            return base
        else:
            raise TypeError('__add__ method expects an object of similar class or child class.')

    def __repr__(self):
        class_string = self.__class__.__name__
        if self.name:
            return "<%s instance '%s' at %s>" % (class_string, self.name, id(self))
        return "<%s instance at %s>" % (class_string, id(self))

    def __str__(self):
        end = ''
        if len(self) > self.max_print_columns:
            end = '..'
        self_str = '<%s(%s):%s%s>' % \
                   (self.name, self.__class__.__name__, list.__repr__(self[:self.max_print_columns]), end)
        if not self:
            return self_str
        checks = [isinstance(child, Root) for child in self]
        if all(checks):
            return '<%s(%s)>\n' % (self.name, self.__class__.__name__) + DIV_0 + self.__str()
        return self_str

    def __str(self):
        """
        Table-style print.
        """
        null_string = ''
        header, values = zip(*[(child.name, child[:]) for child in self])
        max_length = max([len(c) for c in values])
        for i, value in enumerate(values):
            diff = max_length - len(value)
            values[i].extend(['']*diff)
        if self[:]:
            names, lengths, rules = [], [], []

            # make header #
            for item in header:
                if not item:
                    lengths.append(self.min_print_length)
                    names.append('None')
                elif len(item) > self.max_print_length:
                    lengths.append(self.max_print_length)
                    names.append(item[:self.max_print_length-2]+'..')
                elif len(item) < self.min_print_length:
                    lengths.append(self.min_print_length)
                    names.append(item)
                else:
                    lengths.append(len(item))
                    names.append(item)

            # make underscore for header #
            for i in range(len(self)):
                rules.append("-"*lengths[i])

            # format header #
            str_format = " ".join(["%%-%ss" % l for l in lengths])
            result = [str_format % tuple(names)]
            result.append(str_format % tuple(rules))
            for row in zip(*values)[:self.max_print_rows]:
                print_row = []
                for i, value in enumerate(row):
                    if value:
                        if len(str(value)) > lengths[i]:
                            value = str(value)[:lengths[i]-2]+'..'
                    print_row.append(value)
                result.append(str_format % tuple(print_row))
            if len(self[0]) > self.max_print_rows:
                result.append('...')
            return "\n" + "\n".join(result)
        return null_string

    def _load_note(self, other, extra=None):
        """
        Generates a more verbose SQL statement for informational and write purposes. Should allow for replication
            through traceback. This SQL statement will not execute.
        """
        if isinstance(other, Root):
            new_note = """

%s: <
%s>

%s: <
%s>""" % (self.name, self.note, other.name, other.note)
            if extra:
                new_note += str(extra)
            broadcast(msg=new_note, clamor=9)
            self.note = new_note

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if isinstance(value, basestring):
            self._name = self.__clean_name(name=value)
        else:
            TypeError('Name expected to be a basestring class instance, not %s class.' % value.__class__.__name__)

    @name.deleter
    def name(self):
        self._name = self._get_default_name(self.__class__)

    @property
    def parent(self):
        return self._parent

    @parent.setter
    def parent(self, value):
        if self._is_parent(value):
            self._parent = value
        else:
            raise TypeError(PARENT_TYPE_ERROR % (value.__class__.__name__,
                                                 str([x.__name__ for x in self.parent_classes])))

    @parent.deleter
    def parent(self):
        if self._parent is not None:
            del self._parent[self]
        self._parent = None

    @property
    def header(self):
        """ Children's names. """
        return [child.name for child in self]

    @header.setter
    def header(self, value):
        for child, new_name in zip(self, value):
            child.name = new_name

    @header.deleter
    def header(self):
        for child in self:
            del child.name

    def _key_to_index(self, key):
        """
        Complements index and get item methods.
        :param key: Member name or member integer index.
        :return:
        """
        # explicit; duck typing much faster
        # if isinstance(key, (int, long)):
        #     return key
        # ducked

        try:
            key += 0
            return key
        except TypeError:
            pass
        if key in [x.name for x in self if hasattr(x, '_name')]:
            return [x.name for x in self].index(key)
        raise KeyError(CHILD_KEY_ERROR % key)

    def _is_child(self, child):
        """
        "child" is an allowed child class.
        :param child:
        :return:
        """
        if any([isinstance(child, cc) for cc in self.child_classes]):
            return True
        return False

    def _is_parent(self, parent):
        """
        "parent" is an allowable parent class.
        :param parent:
        :return:
        """
        if any([isinstance(parent, pc) for pc in self.parent_classes]):
            return True
        return False

    def index(self, value, start=None, stop=None):
        index = self._key_to_index(key=value)
        return index
    #     if start and stop:
    #         list.index(self, index, start, stop)
    #     elif start:
    #         list.index(self, index, start)
    #     else:
    #         list.index(self, index)

    def append(self, p_object):
        """
        Replaces list append method.
        :param p_object:
        :return:
        """
        if self._is_child(p_object):
            list.append(self, None)
            self[-1] = p_object
            if hasattr(self[-1], 'parent'):
                try:
                    self[-1].parent = self
                except TypeError:
                    pass
            if hasattr(self[-1], 'backends'):
                try:
                    self[-1].backends = self.backends
                except AttributeError:
                    pass
        else:
            raise TypeError(CHILD_TYPE_ERROR % (p_object.__class__.__name__,
                                                str([x.__name__ for x in self.child_classes])))

    def extend(self, iterable):
        if iterable is self:
            iterable = [child for child in self]
        if all([self._is_child(item) for item in iterable]):
            for item in iterable:
                self.append(item)
        else:
            raise TypeError(CHILD_TYPE_ERROR % ('Provided iterable contains a member which ',
                                                str([x.__name__ for x in self.child_classes])))

    def insert(self, key, p_object):
        index = self._key_to_index(key)
        list.insert(self, index, None)
        self[index] = p_object

    def pop(self, key=None):
        index = key
        if index:
            index = self._key_to_index(key)
        # self.__delattr__(index)
        return list.pop(self, index)

    def _set_reserved(self):
        self._reserved = [key for key, value in self.__dict__.items()]

    def __is_reserved(self, name):
        """

        :param name: Name of
        :return: True if name in self._reserved, else False.
        """
        if name in self._reserved:
            return True
        return False

    def __clean_name(self, name):
        """
        Cleans a name for use in .__dict__ so that dot notation can be used.
        """
        # TODO: If names are allowed to be anything dot referencing will not work, but it is very anoying to not be able to use the same names as queried from sql.
        # return clean(raw=name, sql=self.name_clean_level, lower=True).replace(' ', '_')
        return name

    # TODO: Very slow. Re-creating object with self[:] and self.__dict__ may be a better option.
    def copy(self):  # takes a very long time
        try:
            return copy.deepcopy(self)
        except Exception:
            return copy.copy(self)
            # new = self.__class__(children=self)
            # new.__dict__ = self.__dict__
            # return


def check_parent(fn):
    """
    Decorator for SQL methods that require a parent's connection.
    Use when functions first argument is 'connect=(con, crs)'.
    Target must have all optional arguments ('connect=None'). Will error if a connection cannot be determined.
    :param fn:
    :return:
    """
    def try_parent(self, *args, **kwargs):
        if args:
            if any([isinstance(args[0][0], x) for x in
                    [sqlite3.Connection, pyodbc.Connection]]):
                return fn(self, *args, **kwargs)
        elif kwargs:
            if any([isinstance(kwargs.get('connect', (0, 0))[0], x) for x in
                    [sqlite3.Connection, pyodbc.Connection]]):
                return fn(self, *args, **kwargs)
        obj = self
        while hasattr(obj, 'parent'):
            obj = obj.parent
            if hasattr(obj, 'c'):
                return fn(obj, connect=obj.c, *args, **kwargs)
        raise TypeError('Connection object required.')
    return try_parent
# use:
# @check_parent
# def function(self, *args, **kwargs):
#     pass


# TODO: auto generate print column width using value lengths len() for str, figure out what to do with other data types, use the following for int: http://stackoverflow.com/a/2189827/2573907