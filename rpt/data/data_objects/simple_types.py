__author__ = 'drew bailey'
__version__ = 1.0

"""
Reworking typing and variable casting.
"""

from ...util import sample_size, decoder
from decimal import Decimal
from dateutil import parser
import datetime
import inspect
import random
import ast


### TYPE OBJECTS ###

class SimpleTYPE(object):
    """
    Root simple type class, all simple simple_type inherit from this class.
    """
    priority = None
    string = ''

    def __init__(self):
        pass

    def __eq__(self, other):
        """
        :param other: any object with an __eq__ method.
        :return: True/False value is equal to other.
        """
        if type(other) is type(self):
            if self.priority == other.priority:
                return True
            else:
                return False
        else:
            if self.priority == other:
                return True
            else:
                return False

    def __lt__(self, other):
        """
        :param other: any object with a __lt__ method.
        :return: True/False value is less than other.
        """
        if type(other) is type(self.__class__):
            if self.priority < other.priority:
                return True
            else:
                return False
        else:
            if self.priority < other:
                return True
            else:
                return False

    def __gt__(self, other):
        """
        :param other: any object with a __gt__ method.
        :return: True/False value is greater than other.
        """
        if type(other) is type(self.__class__):
            if self.priority > other.priority:
                return True
            else:
                return False
        else:
            if self.priority > other:
                return True
            else:
                return False

    def __nonzero__(self):
        """
        Sets bool(self) return. Relies on value __nonzero__ method.
        """
        if self.priority:
            return True
        return False

    def __str__(self):
        """
        :return:
        """
        return str(self.__class__)

    def __repr__(self):
        """
        :return:
        """
        return str(self.__class__)


class SimpleNULL(SimpleTYPE):
    priority = 0
    string = 'NULL'
    excel_format = ''


class SimpleBLOB(SimpleTYPE):
    priority = 1
    string = 'BLOB'
    excel_format = ''


class SimpleBOOLEAN(SimpleTYPE):
    priority = 1.5
    string = 'BOOLEAN'
    excel_format = ''


class SimpleTEXT(SimpleTYPE):
    priority = 2
    string = 'TEXT'
    excel_format = ''


class SimpleINTEGER(SimpleTYPE):
    priority = 3
    string = 'INTEGER'
    excel_format = '###0'


class SimpleREAL(SimpleTYPE):
    priority = 4
    string = 'REAL'
    excel_format = '#,##0.00'


class SimpleDATETIME(SimpleTYPE):
    priority = 5
    string = 'DATETIME'
    excel_format = 'yyyy-mm-dd'


### EXPLICIT TYPING ###

date_types = [
    SimpleDATETIME,
    datetime.date,
    datetime.time,
    datetime.datetime,
    datetime.tzinfo,
    datetime.timedelta,
    'DATETIME',
    'DATE',
    'TIME',
    'TIMESTAMP',
    'TIMEWITHZONE',
    'TIMESTAMPWITHZONE',
    ]

text_types = [
    SimpleTEXT,
    basestring,
    'TEXT',
    'VARCHAR',
    'CHAR',
    ]

real_types = [
    SimpleREAL,
    float,
    Decimal,
    'REAL',
    'DECIMAL',
    'DOUBLE',
    'NUMBER',
    ]

int_types = [
    SimpleINTEGER,
    int,
    long,
    'INTEGER',
    'SMALLINT',
    'BYTEINT',
    'BIGINT',
    ]

bool_types = [
    SimpleBOOLEAN,
    bool,
    'BOOLEAN',
    ]

# unfinished
interval_types = [
    datetime.timedelta,
    '...',
    ]

# unfinished
null_types = [
    SimpleNULL,
    type(None),
    ]

# unfinished
blob_types = [
    SimpleBLOB,
    ]

_master_types = date_types + text_types + real_types + int_types + bool_types + interval_types + null_types + blob_types
master_types = [x for x in _master_types if isinstance(x, type)]


def bases(cls):
    """
    Gets a classes bases.
    Currently not implemented due to being ~5.9x slower than builtin method inspect.getmro(cls).
    :param cls: an object class.
    :return: a list of that classes base classes (not including the type of cls).
    """
    base_list = list(cls.__bases__)
    for base in base_list:
        base_list.extend(bases(base))
    return list(set(base_list))


def is_type(obj, types):
    """ True if obj is instance of, obj instance is instance of, or obj is type in simple_type. """
    l = []
    for x in types:
        try:
            l.append(isinstance(obj, x))
        except TypeError:
            l.append(False)
        l.append(obj == x)
        try:
            # share common base?
            # l.append(any([isinstance(o, x) for o in bases(obj)]))
            l.append(any([o == x for o in inspect.getmro(obj) if o != object]))
        except Exception:
            l.append(False)
        if True in l:
            break
    return max(l)


def is_date(obj):
    return is_type(obj=obj, types=date_types)


def is_text(obj):
    return is_type(obj=obj, types=text_types)


def is_real(obj):
    return is_type(obj=obj, types=real_types)


def is_int(obj):
    return is_type(obj=obj, types=int_types)


def is_bool(obj):
    return is_type(obj=obj, types=bool_types)


def is_null(obj):
    return is_type(obj=obj, types=null_types)


def __type_num(obj):
    if obj == int(obj):
        return int
    return float


# ok
def __cast_num(obj):
    i = int(obj)
    if obj == i:
        return i
    return float(obj)


def __type_date(obj, known=True):
    if not known:
        obj = __cast_date(obj=obj)
    return type(obj)


# ok
def __cast_date(obj):
    if isinstance(obj, datetime.datetime):
        return obj
    try:
        # how to convert to datetime.datetime..?
        return parser.parse(str(obj))
    except Exception:
        return datetime.datetime(*obj)


def __type_str(obj):
    try:
        # could try fastnumbers c package for better performance here.
        return __type_num(obj=obj)
    except ValueError:
        try:
            obj = ast.literal_eval(str(obj))
        except Exception:
            pass
    return type(obj)


# ok; complex cast...
def __cast_str(obj):
    try:
        # could try fastnumbers c package for better performance here.
        return __cast_num(obj=obj)
    except ValueError:
        try:
            # other object type?
            return ast.literal_eval(str(obj))
        except:
            # handle unicode
            return decoder.decode(obj)


def __fast_cast_str(obj):
    try:
        return str(obj)
    except UnicodeEncodeError:
        return decoder.decode(obj)
    except ValueError:
        return __cast_str(obj=obj)


def __type_null(obj):
    obj = __cast_null(obj)
    return type(obj)


def __cast_null(obj):
    if obj:
        raise TypeError
    return None


def __cast_blob(obj):
    return obj


def explicit_simple_type(py_type, default_type=None):
    """
    Expects a PyType. (result of type(object))
    :return: a simple_type class.
    """
    if default_type:
        return explicit_simple_type(py_type=default_type)
    if is_int(py_type):
        st = SimpleINTEGER
    elif is_real(py_type):
        st = SimpleREAL
    elif is_text(py_type):
        st = SimpleTEXT
    elif is_null(py_type):
        st = SimpleNULL
    elif is_date(py_type):
        st = SimpleDATETIME
    else:
        st = SimpleBLOB
    return st


def explicit_py_type(obj, parse_dates=True):
    """
    Determines best suited Python type out of some general basic types. Returns a cast object and that object's PyType.
    :param obj:
    :param parse_dates:
    :return:
    """
    # if known_type:
    #     if known_type in text_types:
    #         return __get_str_type(obj=obj)
    #     if known_type in int_types+real_types:
    #         return __get_num_type(obj=obj)
    #     if known_type in date_types:
    #         return __get_date_type(obj=obj, known=True)
    if isinstance(obj, type(None)):
        return None, type(None)
    if any([isinstance(obj, t) for t in real_types[:3] + int_types[:3]]):
        return __cast_num(obj=obj), __type_num(obj=obj)
    if any([isinstance(obj, t) for t in date_types[:6]]):
        return __cast_date(obj=obj), __type_date(obj=obj, known=True)
    if parse_dates:
        try:
            return __cast_date(obj=obj), __type_date(obj=obj, known=False)
        except ValueError:
            pass
    if any([isinstance(obj, t) for t in text_types[:2]]):
        return __cast_str(obj=obj), __type_str(obj=obj)
    return obj, type(obj)


def explicit_py_only_type(obj, parse_dates=True):
    """
    Determines best suited Python type out of some general basic types. Returns that PyType.
    :param obj:
    :param parse_dates:
    :return: object's best PyType
    """
    if isinstance(obj, type(None)):
        return type(None)
    if parse_dates:
        try:
            return __type_date(obj=obj)
        except ValueError:
            pass
    if any([isinstance(obj, t) for t in real_types[:3] + int_types[:3]]):
        return __type_num(obj=obj)
    if any([isinstance(obj, t) for t in text_types[:2]]):
        return __type_str(obj=obj)
    if any([isinstance(obj, t) for t in date_types[:6]]):
        return __type_date(obj=obj, known=False)
    return type(obj)


# def explicit_py_type(obj, parse_dates=True):
#     # if known_type:
#     #     if known_type in text_types:
#     #         return __get_str_type(obj=obj)
#     #     if known_type in int_types+real_types:
#     #         return __get_num_type(obj=obj)
#     #     if known_type in date_types:
#     #         return __get_date_type(obj=obj, known=True)
#     if isinstance(obj, type(None)):
#         return type(None)
#     if parse_dates:
#         try:
#             return __get_date_type(obj=obj, known=False)
#         except ValueError:
#             pass
#     if any([isinstance(obj, t) for t in real_types[:3]+int_types[:3]]):
#         return __type_num(obj=obj)
#     if any([isinstance(obj, t) for t in date_types[:6]]):
#         return __get_date_type(obj=obj, known=True)
#     if any([isinstance(obj, t) for t in text_types[:2]]):
#         return __get_str_type(obj=obj)
#     return type(obj)


# CAST TYPE TUPLES FOR __caster
__ct_num = ('value += 0', __cast_num)
__ct_str = ('', __cast_str)
__ct_dat = ('', __cast_date)
__ct_nul = ('if not value is None: raise TypeError()', __cast_null)
__ct_non = ('', __cast_blob)

__cast_types = [
    __ct_num,
    __ct_nul,
    __ct_str,
    __ct_dat,
    __ct_non,
    ]


# def load(iterable, known_type, parse_dates=False):
#     known_type = get_simple_type(iterable=iterable, known_type=known_type)
#     self[:].extend([cast(value=item) for item in iterable])


def cast(value):
    """
    Cast in a way that 'remembers' what worked last to minimize checking.
    :param value: value to cast.
    :return: casted value
    """
    ## Method A:
    # while True:
    #     for test, fn in __cast_types:
    #         try:
    #             exec test
    #             while True:
    #                 try:
    #                     value = yield fn(value)
    #                 except Exception:
    #                     # move onto next type
    #                     break
    #         except Exception:
    #             pass
    ## Method B:
    # l = __cast_types
    # while True:
    #     l_ = []
    #     for i, (test, fn) in enumerate(l):
    #         try:
    #             value = yield fn(value)
    #         except Exception:
    #             # move onto next type
    #             l_.append(i)
    #     l = [__cast_types[i] for i in range(len(__cast_types)) if i not in l_]
    ## Method C:
    if not value and value != 0:
        return None
    for test, fn in __cast_types:
        try:
            exec test
            return fn(value)
        except:
            pass


def cast_known(value, known_simple_type, fast=True):
    """
    Cast generator.
    :param value:
    :param known_simple_type:
    :param fast:
    :return:
    """
    simple_type = explicit_simple_type(known_simple_type)
    if simple_type in(SimpleINTEGER, SimpleREAL):
        test, fn = __ct_num
    elif simple_type == SimpleTEXT:
        if fast:
            test, fn = '', __fast_cast_str
        else:
            # test, fn = __ct_str
            test, fn = '', __fast_cast_str  # TODO: Numeric string conversion is not desired.
    elif simple_type == SimpleNULL:
        test, fn = __ct_nul
    elif simple_type == SimpleDATETIME:
        test, fn = __ct_dat
    elif simple_type == SimpleBLOB:
        test, fn = __ct_non
    else:
        raise TypeError('Unknown known_simple_type.')
    while True:
        try:
            if value or value == 0:
                value = yield fn(value)
            else:
                value = yield None
        except Exception:
            # move onto next type
            value = yield cast(value=value)


def get_simple_type(iterable, known_type=None, parse_dates=True):
    """

    :param iterable:
    :param known_type:
    :param parse_dates: If True; attempts to parse dates.
    :return: A SimpleType object for the iterable set.
    """
    if known_type:
        return explicit_simple_type(known_type)
    size = sample_size(len(iterable))
    sample = random.sample(iterable, size)
    return max([explicit_simple_type(explicit_py_only_type(obj=x, parse_dates=parse_dates)) for x in sample])


# def _apply_type(obj):
#     try:
#         obj += 0
#         yield __cast_num(obj)
#     except:
#         try:
#     yield None
