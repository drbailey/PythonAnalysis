__author__  = 'drew bailey'
__version__ = 0.1

"""

"""

from ...util import make_names_unique, broadcast
from ..data_objects import ExcelReader, ExcelWriterV2
from ..sql import BACKENDS, MASTER_MEMORY
from .root import Root, check_parent
from .index import Index
from .field import Field
import datetime  # used for drop_where eval
import csv


class Table(Root):
    """

    """

    parent_classes = [None, Root]
    child_classes = [Field]
    default_child_name = Root._get_default_name(cls=child_classes[0])

    # ## CLASS METHODS ###
    @classmethod
    def from_csv(cls, file_name, header=True, delimiter=',', quote_char='"', **kwargs):
        """
        Loads a ShelfTable instance with data from a csv file.
        Uses builtin csv package.
        :param file_name: file name (including '.csv'), passed as mimic name
        :param header: if True use first line as header, if list use that as header if not use default vector names,
            defaults to True
        :param delimiter: csv file delimiter, defaults to ',' (comma)
        :param quote_char: csv quote character, defaults to '"', (double quote)
        :return: data, header, name to WorkingTable.__init__
        """
        name = file_name.split('.')[0]
        with open(file_name, 'rb') as f:
            reader = csv.reader(f, delimiter=delimiter, quotechar=quote_char)
            data = [row for row in reader]
            if header is True:
                header = data.pop(0)
            elif not header:
                header = []
            return cls(data, name=name, header=header, **kwargs)

    @classmethod
    def from_excel(cls, file_name, sheet_name, header=True, row_start=0, row_end=None, col_start=0, col_end=None, **kwargs):
        """
        Loads a ShelfTable instance with data from an excel range.
        Row and column start and end points are zero indexed integers.
        Uses xlrd package. ("pip install xlrd" to install).
        :param file_name: file name (including '.xlsx')
        :param sheet_name: worksheet name, passed as mimic name
        :param header: if True use first line as header, if list use that as header if not use default vector names,
            defaults to True
        :param row_start: row start index, defaults to 0
        :param row_end: row end index, defaults to 2,000,000
        :param col_start: column start index, defaults to 0
        :param col_end: column end index, defaults to 2,000,000
        :return: data, header, and name to WorkingTable.__init__
        """
        reader = ExcelReader()
        data = reader.read(file_name=file_name, sheet_name=sheet_name, row_start=row_start, row_end=row_end, col_start=col_start, col_end=col_end)
        # print data
        if header is True:
            header = data.pop(0)
        elif not header:
            header = []
        return cls(data, name=sheet_name, header=header, **kwargs)

    @classmethod
    def from_select(cls, connect, table_name, schema=None, fields=None, where=None, no_case=False, name='', **kwargs):
        """
        Makes a shelf table from a select on a single database table. Essentially a subset.
        :param connect:
        :param schema: A database containing the table selecting from.
            Can be moved once initialized.
        :param table_name: Table name to select.
        :param fields: List of field names to select.
        :param where: list of tuples containing (field, value) pairs, where field = value. [(field, value),]
        :param no_case: A parameter that removes case comparison from sqlite queries, does not work on most ODBC
            connections.
        """
        # if not schema try to get parent?
        sql, values = BACKENDS.generate_select_sql(
            table=table_name,
            schema=schema,
            fields=fields,
            where=where,
            nocase_compare=no_case
        )
        data = BACKENDS.select(
            connect=connect,
            table=table_name,
            schema=schema,
            fields=fields,
            where=where,
            nocase_compare=no_case
        )
        header = BACKENDS.header(connect=connect)
        types = BACKENDS.types(connect=connect)
        if not name:
            name = table_name+'_subset'
        return cls(children=data, name=name, header=header, types=types, note=sql, note_extra=values, **kwargs)

    @classmethod
    def from_sql(cls, connect, sql, name='', values=None, **kwargs):
        """
        Makes a shelf table from passing select SQL to a server connection.
        :param database: A database for table to be a part of with the correct server connection.
            Can be moved once initialized.
        :param name:
        :param sql: SQL to execute, expected to be a select statement.
        :param values: May include values for ? replacements.
        """
        # TODO: What is the best solution for establishing backends pre-init? cls method in Root?
        if kwargs.get('backends'):
            backends = kwargs.get('backends')()
        else:
            backends = BACKENDS
        data = backends.pass_sql(connect=connect, sql=sql, values=values)
        header = backends.header(connect=connect)
        types = backends.types(connect=connect)
        return cls(children=data, name=name, header=header, types=types, note=sql, note_extra=values, **kwargs)

    def __init__(self, children=(), name='', parent=None, header=(), indices=(), **kwargs):
        self._indices = []
        super(Table, self).__init__(name=name, parent=parent)
        known_types = kwargs.get('known_types', False)
        row_first = kwargs.get('row_first', True)

        self.note = kwargs.get('note', '')

        # header = kwargs.get('header', ())
        if not header and children:
            if row_first:
                header = [self.default_child_name+str(i) for i in range(len(children[0]))]
            else:
                header = [self.default_child_name+str(i) for i in range(len(children))]
        elif header:
            header = [str(h) for h in header]
            if not children:
                children = [() for h in header]

        # if rows isinstance of SimpleMatrix or all items in data are SimpleVectors then take values without loading.
        if isinstance(children, self.__class__) or all([isinstance(child, cc) for child in children for cc in self.child_classes]):
            self.__load_simple(obj=children)
        elif children:
            self.__load(obj=children, header=header, known_types=known_types, row_first=row_first)
            self.__load_header(header=header)

        # load indices
        self.indices = indices

    @property
    def simple_types(self):
        """ ... """
        return [t.simple_type for t in self]

    @simple_types.setter
    def simple_types(self, value):
        for c, t in zip(self, value):
            c.simple_type = t

    @simple_types.deleter
    def simple_types(self):
        pass

    @property
    def indices(self):
        """

        """
        return self._indices

    @indices.setter
    def indices(self, value):
        """

        :param value:
        :return:
        """
        self.__load_indices(value)

    @indices.deleter
    def indices(self):
        self._indices = []

    def union(self, other):
        """
        Functions as SQL union for two Matrix objects.
        :param other: Matrix object.
        :return: Resulting Matrix object.
        """
        base = self.copy()
        if not other:
            return base
        other_ = self.__class__(other)
        if len(base) > len(other_):
            for x in range(abs(len(base)-len(other_))):
                other_.append(self.child_classes[0]([None] * len(other_[0])))
        elif len(base) < len(other_):
            for x in range(abs(len(base)-len(other_))):
                base.append(self.child_classes[0]([None] * len(base[0])))
        for row in other_.rows():
            base.append_row(row)
        return base

    def dimensions(self):
        """ Returns columns, rows """
        if self[:]:
            return len(self), len(self[0])
        return 0, 0

    def rows(self):
        # in python 3.x use: list(itertools.izip_longest(*self))
        if len(set([len(child) for child in self])) <= 1:
            return zip(*self)
        return map(None, *self)

    def row(self, index):
        return tuple(child[index] for child in self)

    def iter_rows(self):
        # in python 3.x use: list(itertools.izip_longest(*self))
        if len(set([len(child) for child in self])) <= 1:
            for row in zip(*self):
                yield row
        for row in map(None, *self):
            yield row

    def values(self):
        return [values for field in self for values in field]

    def iter_values(self):
        for field in self:
            for value in field:
                yield value

    def reorder(self, index_list):
        self[:] = [self[index] for index in index_list]

    def move_to_index(self, old_index, new_index):
        """
        Moves a column vector from one index to another.
        :param old_index: current location
        :param new_index: resulting location
        :return: None
        """
        vector = self.pop(old_index)
        list.insert(self, new_index, vector)

    def get_unique_header(self):
        """
        Returns a unique list of column vector names.
        :return: Unique names list.
        """
        return make_names_unique(name_list=[child.name for child in self], base_name=self.default_child_name)

    def make_header_unique(self):
        """
        Forces header to be unique using get_unique_header method.
        :return: None
        """
        for index, item in enumerate(self.get_unique_header()):
            self[index].name = item

    # disabled for now; not controlling length of children.
    # def extend(self, iterable):
    #     if self:
    #         if any([len(o) != len(self[0]) for o in iterable]):
    #             raise IndexError("Vectors of different lengths cannot be added to a table of length %s vectors." % len(self[0]))
    #     Root.extend(self, iterable)

    # disabled for now; not controlling length of children.
    # def append(self, p_object):
    #     if self:
    #         if len(p_object) != len(self[0]):
    #             raise IndexError("Vector of length %s cannot be added to a table of length %s vectors." % (len(p_object), len(self[0])))
    #     Root.append(self, p_object=p_object)

    def extend_rows(self, iterable):
        for row in iterable:
            self.append_row(p_object=row)

    # This method will fail on children of non-uniform length.
    def append_row(self, p_object):
        """

        :param p_object:
        :return:
        """
        if self:
            if len(p_object) != len(self):
                raise IndexError("Row of length %s cannot be appended to a table with %s fields." % (len(p_object), len(self)))
        for index, item in enumerate(p_object):
            self[index].append(item)

    def drop_row(self, row_index):
        for vector in self:
            del vector[row_index]

    def drop_rows(self, row_indices):
        """
        Drops rows from a list of row indices.
        :param row_indices: list of row indices to drop.
        :return:
        """
        indices = list(set(row_indices))
        indices.sort()
        indices.reverse()
        for index in indices:
            self.drop_row(index)

    def drop_rows_where(self, conditions):
        """
        accepts a list tuples containing columns and value conditions as strings
            ex) conditions=[(0, "< 13"), (8, "=='hawaii'")]
        :param conditions: [(column_index, evaluation to apply as string),]
        :return: None
        """
        conditions_ = []
        for condition in conditions:
            column_index = self.index(condition[0])
            conditions_.append(str("row[%s] %s" % (column_index, condition[1])))
        drop_indices = [i for i, row in enumerate(self.rows()) if all(map(eval, conditions_))]
        if drop_indices:
            self.drop_rows(row_indices=drop_indices)

    # ## LOAD METHODS ###
    def __load(self, obj, header=(), row_first=True, known_types=None):
        vectors = obj
        if row_first:
            vectors = zip(*obj)
        self.__load_base(vectors=vectors, header=header, known_types=known_types)

    def __load_base(self, vectors, header,  known_types):
        for i, (vector, name) in enumerate(zip(vectors, header)):
            if known_types:
                vector = self.__load_field(vector=vector, known_type=known_types[i])
            else:
                if name not in self.default_child_name:
                    parse_dates = Field.likely_date_column(name=name)
                elif hasattr(vector, 'name'):
                    parse_dates = Field.likely_date_column(name=vector.name)
                else:
                    parse_dates = False
                vector = self.__load_field(vector=vector, parse_dates=parse_dates)
            self.append(vector)

    def __load_simple(self, obj):
        """
        This method bypasses all checks and filters to append elements as though it were a simple list. Many other
        methods could fail as a result of this load type.
        :param obj:
        :return:
        """
        if obj:
            self[:] = obj[:]

    @staticmethod
    def __load_field(vector, known_type=None, parse_dates=False):
        return Field(children=vector, known_type=known_type, parse_dates=parse_dates)

    def __load_header(self, header):
        if not self[:]:
            for h in header:
                self.append(Field(children=[], name=h))
        else:
            for i, h in enumerate(header):
                if h:
                    self[i].name = h

    def __load_indices(self, indices):
        self._indices = [self.__load_index(index) for index in indices]

    # TODO: check that indexed fields are in self?
    @staticmethod
    def __load_index(index):
        if isinstance(index, Index):
            return index
        raise TypeError('Invalid index %r; Indices are expected to be an instance of class Index.' % index)

    def add_index(self, index):
        self._indices.append(self.__load_index(index=index))

    def remove_index(self, index):
        if isinstance(index, int):
            del self._indices[index]
        else:
            try:
                i = self._indices.index(index)
                del self._indices[i]
            except ValueError:
                i = [i.name for i in self._indices].index(index)
                del self._indices[i]

    def to_csv(self, file_name='', delimiter=',', quote_char='"', header=True):
        if not file_name:
            if self.name:
                name = self.name
            else:
                name = '_table_data'
            file_name = name+'.csv'
        with open(file_name, 'wb') as f:
            c = csv.writer(f, delimiter=delimiter, quotechar=quote_char, quoting=csv.QUOTE_MINIMAL)
            if header:
                c.writerows([self.header] + [row for row in self.rows()])
            else:
                c.writerows(self.rows())

    def to_excel(self, file_name='', sheet_name=None, row_start=0, col_start=0):
        if not file_name and self.name:
            file_name = str(self.name) + '.xlsx'
        else:
            file_name = '_table_data.xlsx'
        if not sheet_name:
            sheet_name = self.name
        w = ExcelWriterV2()
        w.write(data_list=[self], file_name=file_name, sheet_name=sheet_name, row_start=row_start, col_start=col_start)

    # TODO: if self+other above some size limit use MASTER instead of MASTER_MEMORY for merge database.
    # split to different functions? are these tasks reused?
    # @check_parent
    def join(self, other=None, how='union', on=((), ), connect=MASTER_MEMORY):
        """

        :param other:
        :param how:
        :param on:
        :param connect:
        :return:
        """
        how = how.lower()
        # if union perform operation in python. #
        if how == 'union':
            return self.union(other=other)

        # generate joined table name #
        temp_str = '%s_temp'
        self_temp_name, other_temp_name = temp_str % self.name, temp_str % other.name
        if self_temp_name == other_temp_name:
            other_temp_name += '_'
        joined_table_name = self.name+'_'+how.upper()+'_'+other.name

        # generate unique names #
        self_join_header = self.get_unique_header()
        other_join_header = other.get_unique_header()
        self_join_indices = [self.index(left) for left, right in on]
        other_join_indices = [other.index(right) for left, right in on]
        join_on = [(self_join_header[left], other_join_header[right]) for left, right in zip(self_join_indices, other_join_indices)]

        # write both tables to merge database #
        if_not = True
        self.db_write(connect=connect, name=self_temp_name, if_not=if_not)
        other.db_write(connect=connect, name=other_temp_name, if_not=if_not)

        # determine join connection object #
        # some logic using size in memory?

        # generate, execute, and enhance sql for new object as result #
        sql = self.__generate_join_sql(self_name=self_temp_name, other_name=other_temp_name, how=how, on=join_on)
        result = Table.from_sql(connect=connect, sql=sql, name=joined_table_name, parent=self.parent)
        # result.note = self.note.__generate_joined_sql(other=other, sql=sql) #  notes not yet implemented

        # restore header #
        final_header = self.header+other.header
        for i, name in enumerate(final_header):
            result[i].name = name
        join_on = [(self.header[left], other.header[right]) for left, right in zip(self_join_indices, other_join_indices)]

        # drop join fields from result #
        self.__drop_redundant_fields(obj=result, how=how, on=join_on)

        return result

    @staticmethod
    def __generate_join_sql(self_name, other_name, how, on):
        """
        Generates a functional sql statement for a join.
        :param self_name:
        :param other_name:
        :param how:
        :param on:
        :return:
        """
        db_type = 'lite'  # if other join engines are later implemented.
        if db_type == 'lite':
            sql = BACKENDS.generate_lite_join_sql(table1=self_name, table2=other_name, how=how, on=on)
        # elif db_type == 'odbc':
        #     sql = BACKENDS.generate_odbc_join_sql(table1=self.full_name(), table2=other.full_name(), how=how, on=on,
        #                                           database=self.parent.name)
        else:
            raise NotImplementedError("Only db_types 'lite' is currently implemented.")
        broadcast(msg=sql, clamor=7)
        return sql

    @staticmethod
    def __drop_redundant_fields(obj, how, on):
        """
        Drop redundant fields joined on.
        :param how:
        :param on:
        :return:
        """
        if how == 'outer':
            return
        elif how == 'right':
            # drop leftmost fields
            for field in [left for left, right in on]:
                del obj[field]
        else:
            # drop rightmost field with that name
            obj.reverse()
            for field in [right for left, right in on]:
                del obj[field]
            obj.reverse()

    # @staticmethod
    # def __get_restore_header(header, current_header, how, on):
    #     """
    #     Restores an object header from the all .lower() applied in a no-case compare.
    #     :param header:
    #     :param how:
    #     :param on:
    #     :return:
    #     """
    #     if how == 'outer':
    #         pass
    #     elif how == 'right':
    #         # drop leftmost fields
    #         for field in [left for left, right in on]:
    #             del header[current_header.index(field)]
    #     else:
    #         # drop rightmost field with that name
    #         header.reverse()
    #         for field in [right for left, right in on]:
    #             del header[current_header.index(field)]
    #         header.reverse()
    #     return header

    @check_parent
    def db_drop(self, connect=None):
        BACKENDS.drop_table(connect=connect, table=self.name)

    @check_parent
    def db_write(self, connect=None, name=None, if_not=True):
        if not name:
            name = self.name
        header = self.get_unique_header()
        BACKENDS.create_table(connect=connect, table=name, fields=header,
                              data_types=[t.string for t in self.simple_types], if_not=if_not)
        self.db_insert(connect=connect, name=name)

    @check_parent
    def db_insert(self, connect=None, name=None):
        if not name:
            name = self.name
        if self[:]:
            BACKENDS.insert_rows(connect=connect, table=name, rows=self.rows())

    @check_parent
    def db_update(self, connect=None):
        self.db_drop(connect=connect)
        self.db_write(connect=connect, if_not=False)

    @check_parent
    def db_drop_rows(self, connect=None, where=((),)):
        """
        Where currently only supports drops where field=value. (Equals only)
        :param connect:
        :param where: [(field_1, value_1), (), ...]
        :return:
        """
        BACKENDS.drop_rows(connect=connect, table=self.name, where=where)

    @check_parent
    def db_reindex(self, connect=None):
        for index in self.indices:
            index.db_update(connect=connect)

    @check_parent
    def db_move(self, connect=None):
        # TODO: move from lite table to other lite database.
        # move from database to another...?
        pass

    @check_parent
    def db_backup(self, connect=None):
        pass

    # child methods #
    # @check_parent
    # def db_drop_field(self, connect=None, field=None):
    #     self[field].db_drop(connect=connect, table=self.name)
    #
    # @check_parent
    # def db_rename_field(self, connect=None, field=None):
    #     self[field].db_rename(connect=connect, table=self.name)
    #
    # @check_parent
    # def db_move_field(self, connect=None, field=None, new_table=None):
    #     self[field].db_move(connect=connect, old=self.name, new=new_table)

    # def __setitem__(self, key, value):
    #     try:
    #         Root.__setitem__(self, key=key, value=value)
    #     except TypeError:
    #         if isinstance(value, Index):
    #             pass
    #         else:
    #             raise TypeError('wrong type... ')
    #
    # def __setattr__(self, key, value):
    #     pass

    def rank(self, partition_by, order_by, desc=False):
        """
        Ranks a table by order by fields when grouped on partition by fields.
        :param partition_by: list of keys to partition over.
        :param order_by: list of keys to order by.
        :param desc: if True reverses sort order, making sort highest to lowest.
        :return: None
        """
        self.__mulit_rank(partition_by=partition_by, order_by=order_by, desc=desc)

    def __simple_rank(self, partition_by, order_by, desc=False):
        """
        Accepts 2 keys and a bool.
        :param partition_by:
        :param order_by:
        :param desc:
        :return:
        """
        part = self[partition_by]
        order = self[order_by]
        new = [None] * len(self[0])
        for item in set(part):
            root_indices, rank_indices, elements = [], [], []
            for i, x in enumerate(order):
                if part[i] == item:
                    root_indices.append(i)
                    elements.append(x)
            rank_indices = [sorted(elements, reverse=desc).index(x) for x in elements]
            for root, rank in zip(root_indices, rank_indices):
                new[root] = rank + 1
        if desc:
            field_name = 'rank_%s_by_%s_desc' % (partition_by, order_by)
        else:
            field_name = 'rank_%s_by_%s' % (partition_by, order_by)
        self[field_name] = new

    # TODO build to support multiple order keys
    def __mulit_rank(self, partition_by, order_by, desc=False):  # DOESN'T SUPPORT MULTIPLE ORDER BY KEYS
        """
        Accepts 2 lists of key(s) and bool.
        :param partition_by:
        :param order_by:
        :param desc:
        :return:
        """
        part = [self[key] for key in partition_by]
        order = [self[key] for key in order_by][0]  # single order by, for now...
        new = [None] * len(self[0])
        if len(part) > 1:
            part = zip(*part)
        for item in set(part):
            root_indices, rank_indices, elements = [], [], []
            for i, x in enumerate(order):
                if part[i] == item:
                    root_indices.append(i)
                    elements.append(x)
            rank_indices = [sorted(elements, reverse=desc).index(x) for x in elements]
            for root, rank in zip(root_indices, rank_indices):
                new[root] = rank + 1
        if desc:
            field_name = 'rank_%s_by_%s_desc' % ('many', order_by[0])
        else:
            field_name = 'rank_%s_by_%s' % ('many', order_by[0])
        self[field_name] = new

    def sort_by(self, key_tuples):
        """
        ex) key_tuples = [(key1, 'desc'), (key2, 'asc'), (key3, 'desc')]
        :param key_tuples:
        :return:
        """
        d = {'desc': True,
             'asc': False}
        keys, orders = zip(*key_tuples)
        keys = [self.index(key) for key in keys]
        keys.reverse()
        orders = [d.get(order.lower(), order) for order in orders]
        orders.reverse()
        if not all([type(order) == bool for order in orders]):
            print "Invalid order specification; each key should include 'asc' or 'desc' order specification."
            print orders
            return
        self['sort_index_field'] = range(len(self[0]))
        s = self.rows()
        for key, order in zip(keys, orders):
            s = sorted(s, key=lambda x: x[key], reverse=order)
        sorted_indices = zip(*s)[-1]
        del self['sort_index_field']
        for index in range(len(self)):
            self[index][:] = [self[index][i] for i in sorted_indices]

    def select(self, sql, values=None, **kwargs):
        self.db_write(connect=MASTER_MEMORY, name=self.name, if_not=False)
        t = Table.from_sql(connect=MASTER_MEMORY, sql=sql, values=values, name=kwargs.get('name', self.name), **kwargs)
        self.db_drop(connect=MASTER_MEMORY)
        return t
