__author__  = 'drew bailey'
__version__ = 1.0

"""
Simplified Vector object designed to be inherited by database Field objects. Properties and methods overlapping with
 Root or Field database objects may have been removed or altered.
"""

from .simple_types import *
import itertools
import copy
import re
from ...config import SQL_CLEAN_LEVEL


likely_dates = ['dt', 'date', 'dates', 'dts', 'time', 'run']
likely_dividers = ['^', '$', ' ', '_']


class SimpleVector(list):
    """
    Vector data_objects are designed to represent the COLUMN VECTORS that make up Matrix data_objects.
    zip(*columns) (or the Matrix.rows() method) will efficiently return a list of tuples that can be used as rows.
    Vectors can however be used as any n-dimensional object.
    """

    name_clean_level = SQL_CLEAN_LEVEL
    fast_load_cutoff = 500

    def __init__(self, elements=None, **kwargs):
        """
        ...
        :param elements: Iterable of vector elements.
        :param parse_dates: Will attempt to parse all elements to datetime data_objects if strings.
        :param known_type: If the type of the field is already known (via SQL cursor or
        :param fast:
        :param name_clean_level: How many restrictions to apply for SQL keyword and character cleaning.
        :param fast_cutoff:
        :return:
        """
        super(SimpleVector, self).__init__()
        self._simple_type = None
        parse_dates = kwargs.get('parse_dates', False)
        known_type = kwargs.get('known_type', None)
        self.name_clean_level = kwargs.get('name_clean_level', self.name_clean_level)
        fast = kwargs.get('fast', None)

        if elements:
            if fast is None and len(elements) > self.fast_load_cutoff:
                fast = True
            self.load_vector(elements=elements, parse_dates=parse_dates, known_type=known_type, init=True, fast=fast)

    @property
    def simple_type(self):
        return self._simple_type

    @simple_type.setter
    def simple_type(self, other):
        self._simple_type = explicit_simple_type(py_type=other)

    @simple_type.deleter
    def simple_type(self):
        self._simple_type = None

    @staticmethod
    def likely_date_column(name):
        """
        Enumerates all outcomes of likely_divider, likely_date, likely_divider combos, if column name is in this return
        True, else return False.
        :param name: Column name to test
        :return: boolean, True if likely date, else False
        """
        l = [likely_dividers, likely_dates, likely_dividers]
        all_likely = [''.join(x) for x in list(itertools.product(*l))]
        if name:
            for rule in all_likely:
                if re.search(rule, name.lower()):
                    return True
        return False

    def load_vector(self, elements, known_type=None, parse_dates=False, init=False, fast=True):
        """
        Loads iterable into self.
        :param elements:
        :param known_type:
        :param parse_dates:
        :param init:
        :param fast: fast string load, doesn't grab abstract data types or parse dates.
        :return:
        """

        # determine simple_type
        if known_type:
            known_type = explicit_simple_type(known_type)
        else:
            known_type = get_simple_type(iterable=elements, known_type=known_type, parse_dates=parse_dates)
        # setup generator
        cast_gen = cast_known(value=0, known_simple_type=known_type, fast=fast)
        cast_gen.send(None)
        # load
        if init:
            self[:] = [cast_gen.send(x) for x in elements]
            self.simple_type = known_type
        else:
            # extend or add to self? (which is faster?)
            self[:] += [cast_gen.send(x) for x in elements]
            self.simple_type = max([self.simple_type, known_type])
            # list.extend(self, [cast_gen.send(x) for x in elements])

        # for element in elements:
        #     self.load_element(element=element, parse_dates=parse_dates, known_type=known_type)
        # if init:
        #     if known_type:
        #         e, t = explicit_known_type(obj=elements[0], known_type=known_type)
        #         self._simple_type = explicit_simple_type(py_type=t)
        #     else:
        #         size = sample_size(len(elements))
        #         sample = random.sample(elements, size)
        #         self._simple_type = max([explicit_py_type(parse_dates=parse_dates)])
        #     return
        #
        # return
        # e_, t_ = [], []
        # for element in elements:
        #     e, t = self.__load_element(element=element, parse_dates=parse_dates, known_type=known_type)
        #     e_.append(e)
        #     t_.append(t)
        # self._simple_type = max(t_)
        # list.extend(self, e_)

    # def __load_element(self, element, known_type=None, parse_dates=False):
    #     if known_type:
    #         element, py_type = explicit_known_type(obj=element, known_type=known_type)
    #     else:
    #         element, py_type = explicit_py_type(obj=element, parse_dates=parse_dates)
    #     simple_type = explicit_simple_type(py_type=py_type)
    #     return element, simple_type

    def load_element(self, element, known_type=None, fast=True):
        if known_type:
            known_type = explicit_simple_type(known_type)
        else:
            known_type = self.simple_type
        cast_gen = cast_known(value=0, known_simple_type=known_type, fast=fast)
        cast_gen.send(None)
        list.append(self, cast(element))
        self._simple_type = max(known_type, self._simple_type)
        # cleaning removed due to marginal benefit and poor object load performance. (2015.05.07)
        # if simple_type in [SimpleTEXT, SimpleNONE, SimpleNULL]:
        #     element = clean(raw=element, sql=self.name_clean_level)

    def clear(self, full=False):
        """
        Clears self.
        :param full: If full self becomes [], else it becomes [None]*len(self)
        :return: None
        """
        if full:
            del self[:]
        else:
            cleared = [None]*len(self)
            del self[:]
            list.extend(self, cleared)

    def gtin_to_upc(self):
        """ Drops the last digit of each element in a vector. Skips None type data_objects. """
        # l = []
        # for element in self:
        #     if element:
        #         l.append(int(str(int(element))[:-1]))
        # self.clear(full=True)
        # self.extend(l)

        # faster ...
        # for i, element in enumerate(self[:]):
        #     if element:
        #         self[i] = element // 10

        # fastest ...
        self[:] = [element // 10 for element in self[:]]

    def upc_to_gtin(self):
        # l = []
        # for element in self:
        #     l.append(self.__compute_check_digit(element))
        # self.clear(full=True)
        # self.extend(l)

        # faster ...
        for i, element in enumerate(self[:]):
            if element:
                self[i] = self.__compute_check_digit(element)

    def x_to_mfc_cd(self):
        """
        Drops all but the left five digits of each element in a vector. Skips None type data_objects.
         - 'mfc cd' stands for manufacturer code.
        """
        # l = []
        # for element in self:
        #     if element:
        #         l.append(int(str(int(element))[:5]))
        # self.clear(full=True)
        # self.extend(l)

        # faster ...
        def converter(n):
            while n >= 10000000000: # yea... unlimited size int supported :)
                n /= 1000000
            if n >= 10000000:
                n /= 1000
            if n >= 1000000:
                n /= 100
            if n >= 100000:
                n /= 10
            return n

        self[:] = [converter(n) for n in self[:]]
        for i, element in enumerate(self[:]):
            if element:
                self[i] = converter(element)

    def slu_to_upc(self):
        """
        Adds five zeros to the end of a SLU.
        """
        # l = []
        # for element in self:
        #     if element:
        #         l.append(int(str(int(element))+'00000'))
        # self.clear(full=True)
        # self.extend(l)

        # faster ...
        for i, element in enumerate(self[:]):
            if element:
                self[i] = element * 100000

    def slu_to_gtin(self):
        """
        Adds five zeros to the end of a SLU then computes a check digit.
        """
        # l = []
        # for element in self:
        #     l.append(self.__compute_check_digit(int(str(int(element))+'00000')))
        # self.clear(full=True)
        # self.extend(l)

        # faster ...
        for i, element in enumerate(self[:]):
            if element:
                self[i] = self.__compute_check_digit(element * 100000)

    @staticmethod
    def __compute_check_digit(upc, n=14):
        """
        Compute the check digit for an n digit UPC code.
        :param upc: Code to compute check digit for.
        :return: GTIN code, (UPC + Check Digit).
        """
        gtin = upc
        if upc:
            upc_ = str(int(upc)).strip()
            diff = (n-1)-len(upc_)
            upc_ = '0' * diff + upc_
            odd, even = 0, 0
            for i, digit in enumerate(upc_):
                if (i+1) % 2 == 0:
                    even += int(digit)
                else:
                    odd += int(digit)
            odd *= 3
            result = 10 - ((odd+even) % 10)
            if result == 10:
                result = 0
            gtin = int(upc_+str(result))
        return gtin

    def extend(self, iterable):
        for item in iterable:
            self.append(item)

    def append(self, item):
        self.load_element(element=item)

    def make_date(self):
        self._simple_type = SimpleDATETIME
        self.load_vector(elements=self[:], known_type=self.simple_type, parse_dates=True, init=True, fast=True)

    def copy(self):
        return copy.deepcopy(self)

    # TODO: finish this method.
    def replace(self, old, new):
        # for element in self do string replacement.
        pass
