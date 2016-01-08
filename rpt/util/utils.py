__author__ = 'drew bailey'
__version__ = 0.8

"""
Utilities handling a variety of miscellaneous tasks.
Includes:
    Clean PATH changes.
    String processing.
    Date to string and string to date transforms.
    Unknown extension handling.
    Nested dictionary unraveling and printing.
    Manual garbage collection.
"""

from ..config import SQL_RESERVED, SQL_REPLACE, DATE_FORMAT as dt_format, EXTENSION_PRIORITY, ENCODINGS
from .broadcast import broadcast
import unicodedata
import datetime
import copy
import sys
import os
import re
import gc


name_extension_order = 'abcdefghijklmnopqrstuvwxyz'


class Path():
    """
    Path class allows for a PATH change using a python 'with'.
    This enables quick and easy clean_up_app back to origin PATH.
    """
    def __init__(self, new=None):
        """
        __init__ defaults to '..' back one directory, jumps
        to a new PATH if new contains '\\', or appends to
        current directory if string not containing '\\'.
        """
        self.mPath = os.getcwd()
        if new:
            if new.find('\\') > -1:
                self.nPath = new
            else:
                self.nPath = self.mPath+'\\'+new
                # print 'from Path(): ',self.nPath
        else:
            self.nPath = '..'
            self.nPath = self.mPath+'\\'+self.nPath

    def __enter__(self):
        self.change()

    def __exit__(self, type, value, traceback):
        self.change(False)

    def change(self, forward=True):
        if forward:
            sys.path.insert(0, self.nPath)
            os.chdir(self.nPath)
        else:
            sys.path.pop(0)
            os.chdir(self.mPath)


def path(path):
    """
    changes PATH to given PATH.
    Allows for PATH changes without importing os globally.
    """
    import os
    os.chdir(path)


class Clean():
    def __init__(self, sql=0, lower=False, length_limit=None):
        """
        Formats strings to exclude sql keywords and restricted characters and converts from unicode to ascii.
        :param sql: -1 -> do not clean for sql restricted key words or characters.
                     0 -> only replace characters that interfere with the actual injection string (').
                     1 -> clean for disallowed header characters.
                     2 -> clean for disallowed header characters and sql key words.
        """
        self.lower = lower
        self.sql = sql
        self.length_limit = length_limit
        self._replace = SQL_REPLACE
        self._sql_reserved = self.__list_to_regex_dict(a_list=SQL_RESERVED, append_char='_')

    def process(self, raw, sql=None, lower=None, length_limit=None):
        if not sql or sql == 0:
            sql = self.sql
        if not lower:
            lower = self.lower
        if not length_limit:
            length_limit = self.length_limit

        if isinstance(raw, basestring):
            return self.__master_clean(raw=raw, sql=sql, lower=lower, length_limit=length_limit)
        else:
            return raw

    @staticmethod
    def __list_to_regex_dict(a_list, append_char):
        dictionary = {}
        for item in a_list:
            dictionary['\s'+str(item)+'\s'] = append_char+str(item)
        return dictionary

    def __master_clean(self, raw, sql=None, lower=None, length_limit=None):
        """
        Replaces restricted characters and key words. Can also restrict maximum length or convert to lower.
        :return: Cleaned string.
        """
        # decode
        # string = self._decode(self.raw)
        string = decoder.decode(string=raw, ignore=True)

        # sql
        if sql == 2:
            string = self._replacer(replace=self._sql_reserved, raw_string=string)
            string = self._replacer(replace=self._replace, raw_string=string)
        elif sql == 1:
            string = self._replacer(replace=self._replace, raw_string=string)
        elif sql == 0:
            # string = string.replace("'", "''")
            string = re.sub("'+", "''", string)

        elif sql == -1:
            pass
        else:
            print '%s is not a recognized sql input. Please use -1, 0, 1, or 2.' % sql

        # lower
        if lower:
            string = string.lower()

        # length
        if length_limit:
            string = string[:int(length_limit)]

        return string

    @staticmethod
    def _replacer(replace, raw_string):
        """ Case insensitive regex search and replace based on dictionary keys and values. """
        new_string = raw_string
        for k, v in replace.items():
            new_string = re.sub('(?i)'+str(k), str(v), new_string)
        return new_string


class Decoder(object):
    """
    Unicode arbitrary decoder. Adapted from:
    http://stackoverflow.com/questions/1715772/best-way-to-decode-unknown-unicoding-encoding-in-python-2-5
    """

    utf8_detector = re.compile(r"""^(?:
        [\x09\x0A\x0D\x20-\x7E]            # ASCII
      | [\xC2-\xDF][\x80-\xBF]             # non-overlong 2-byte
      |  \xE0[\xA0-\xBF][\x80-\xBF]        # excluding overlongs
      | [\xE1-\xEC\xEE\xEF][\x80-\xBF]{2}  # straight 3-byte
      |  \xED[\x80-\x9F][\x80-\xBF]        # excluding surrogates
      |  \xF0[\x90-\xBF][\x80-\xBF]{2}     # planes 1-3
      | [\xF1-\xF3][\x80-\xBF]{3}          # planes 4-15
      |  \xF4[\x80-\x8F][\x80-\xBF]{2}     # plane 16
     )*$""", re.X)
    cp1252_detector = re.compile(r'[\x80-\xBF]', re.X)
    xa4_detector = re.compile(r'\xA4', re.X)
    other_encodings = ENCODINGS

    def __init__(self):
        pass

    def to_unicode(self, string):
        """ make unicode """
        try:
            if re.match(self.utf8_detector, string):
                return unicode(string, 'utf_8')
            if re.match(self.cp1252_detector, string):
                if re.match(self.xa4_detector, string):
                    return unicode(string, 'iso8859_15')
                else:
                    return unicode(string, 'cp1252')
            return unicode(string, 'latin_1')
        except TypeError:
            try:
                for encoding in self.other_encodings:
                    unicode_string = unicode(string, encoding)
                    print string, 'decoded to', unicode_string, 'using encoding', encoding
                    return unicode_string
            except:
                raise UnicodeError("Still unknown encoding after attempting do guess all ENCODINGS known to package.")

    def decode(self, string, encoding='ascii', ignore=False):
        unknowns = 'replace'
        if ignore:
            unknowns = 'ignore'
        if type(string) == unicode:
            try:
                string = str(string)
                unicode_string = self.to_unicode(string=string)
            except UnicodeEncodeError:
                unicode_string = string
        else:
            unicode_string = self.to_unicode(string=string)

        return unicodedata.normalize('NFD', unicode_string).encode(encoding, unknowns)


decoder = Decoder()
cleaner = Clean()


def clean(raw, sql=0, lower=False, length_limit=None):
    return cleaner.process(raw=raw, sql=sql, lower=lower, length_limit=length_limit)


def get_date():
    """ Gets yesterday without import clean_up_app. """
    # dt = datetime.date.today() - datetime.timedelta(days=1)
    dt = datetime.date.today()
    return dt


def transform_string_date(dtat, date_format=None):
    """
    Given a string of format _dformat returns datetime object,
    given a date returns string of format _dformat.
        _dformat is held in rpt.util.config.py
    """  # %self._dformat # use decorator function @alterDoc..?
    if not date_format:
        date_format = dt_format
    if isinstance(dtat, basestring):
        try:
            dtat = re.search('^(.+)\.', str(dtat)).groups()[0]
        except:
            dtat = str(dtat)
        d = datetime.datetime.strptime(dtat, date_format)
    else:
        d = dtat.strftime(date_format)
    return d


def ambiguous_extension(filename):
    """
    Allows for * extension simple_type. Checks files in alphabetical order.
    ex) filename='test.xls*'
        checks for test.xlsm, then .xlsx, then .xls returning a match when found.
    :param filename: Filename ending in *
    :return: If working filename is found returns that name, else returns initial filename.
    """
    broadcast(msg='Matching extension %s in %s.' % (filename.split('.')[-1], os.getcwd()), clamor=8)
    f_ = filename[:-1]
    for letter in EXTENSION_PRIORITY:
        f = f_+letter
        if os.path.isfile(f) and os.access(f, os.R_OK):
            return f
    if os.path.isfile(f_) and os.access(f_, os.R_OK):
        return f_
    broadcast('no valid extension found for %s.' % filename, clamor=6)
    return filename


def collect_garbage():
    """ Forces python to garbage collect. """
    gc.collect()


def dict_explore(d):
    """ explores a dictionary object. """
    for k, v in d.items():
        print 'KEY: \n', k
        try:
            print '-'*70
            print '-'*70
            for k1, v1 in v.items():
                print '%s SUBKEY: \n' % k.upper(), k1
                print '%s VALUES: \n' % k1.upper(), v1
                print '-'*70
        except:
            print 'VALUES: \n', v


def length_check(*args):
    """
    Compares the lengths of arguments.
    Expects all iterable arguments.
    :return: (
        -1; if not any(args)
        1; if all len(args) are equal
        0; else
        ,
        longest object length as integer,
        longest object as input)
    """
    # if no arguments return
    return_null = (-1, 0, None)
    if not args:
        return return_null
    # if nothing return
    if not any(args):
        return return_null
    # else check all object lengths
    return_flag = 1
    return_length = 0
    return_object = args[0]
    for i, arg in enumerate(args):
        if not arg:
            continue
        argument_length = len(arg)
        if argument_length != return_length:
            if i > 0:
                return_flag = 0
            if argument_length > return_length:
                return_length = argument_length
                return_object = arg
    return return_flag, return_length, return_object


# currently doesn't use base_name parameter
def make_names_unique(name_list, base_name=None, no_case=True):
    """
    Return a list of names that are made unique by appending letters to a base.
    """
    new_name_list = name_list[:]
    for i in range(len(new_name_list)):
        base_name = new_name_list[i]
        ext_index = 1
        if no_case:
            while new_name_list[i].lower() in [name.lower() for name in new_name_list[:i]]:
                print i, new_name_list
                new_name_list[i] = base_name + extension_from_index(ext_index)
                ext_index += 1
        else:
            while new_name_list[i] in new_name_list[:i]:
                new_name_list[i] = base_name + extension_from_index(ext_index)
                ext_index += 1
    return new_name_list


def extension_from_index(index):
    length = len(name_extension_order)
    extension = ''
    while index > length-1:
        extension += name_extension_order[-1]
        index -= length
    extension += name_extension_order[index]
    return extension


### UNFINISHED TO TYPE CLASS
# class ToType():
#     def __init__(self, value):
#         self.raw = value
#         self.value = None
#
#     def process(self):
#         value = self.unpack(self.raw)
#         value = self.str_to_digit(value)
#
#
#     @staticmethod
#     def unpack(value):
#         try:
#             value = value.value
#         except:
#             value = value
#         return value
#
#     @staticmethod
#     def str_to_digit(string):
#         if '.' in string:
#             try:
#                 value = float(string)
#             except ValueError:
#                 try:
#                     value = int(string)
#                 except:
#                     pass

# x = ast.literal_eval(x)