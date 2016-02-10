__author__  = 'drew bailey'
__version__ = 0.2

"""
Excel read, write, and copy functions for mimic lite object use;
using xlrd, XlsxWriter, and win32com respectively.
"""

# from ...com import packages
from ...util import make_names_unique
import xlsxwriter
# import win32com
import xlrd


class Reader(object):
    """
    I do reading things.
    """


class ExcelReader(Reader):
    """
    Reads excel files.
    """

    def __init__(self):
        pass

    def read(self, file_name, sheet_name, row_start=0, row_end=None, col_start=0, col_end=None):
        if not row_end:
            row_end = 2000000
        if not col_end:
            col_end = 2000000
        wb = xlrd.open_workbook(file_name)
        ws = wb.sheet_by_name(sheet_name)
        data = []
        for row_index in range(ws.nrows)[row_start:row_end+1]:
            data_row = ws.row_values(row_index)[col_start:col_end+1]
            row_types = ws.row_types(row_index)[col_start:col_end+1]
            if 3 in row_types:
                for i, t in enumerate(row_types):
                    if t == 3:
                        data_row[i] = xlrd.xldate.xldate_as_datetime(xldate=data_row[i], datemode=wb.datemode)
            data.append(data_row)
        return data


class Writer(object):
    """
    I do writing things.
    """


class ExcelWriter(Writer):
    """
    Writes data_objects to excel.
    """

    font = 'Consolas'
    font_size = 11
    # width_conversion = font_size * 1.1
    width_conversion = 1.3
    header_style = {'font': font,
                    'size': font_size,
                    'bold': True,
                    'bottom': 1,
                    }
    body_style = {'font': font,
                  'size': font_size,
                  }

    constant_memory = True
    formatting = True
    max_column_width = 30

    write_sql = True

    def __init__(self):
        pass

    def write(self, mimic_list, file_name, sheet_name=None):
        # # open wb
        wb = xlsxwriter.Workbook(file_name, {'constant_memory': self.constant_memory})
        # # define header_style
        header_style = wb.add_format(self.header_style)
        # # write each mimic to its own sheet
        for i, mimic in enumerate(mimic_list):
            # # get sheet name
            if not sheet_name and mimic.name:
                this_sheet_name = mimic.name
            else:
                this_sheet_name = 'Data%s' % i
            # # make sure name doesn't already exist in workbook (fatal error) ##
            this_sheet_name = make_names_unique(name_list=[ws.name for ws in wb.worksheets()]+[this_sheet_name],
                                                base_name=this_sheet_name, no_case=True)[-1]
            # # define worksheet
            ws = wb.add_worksheet(name=this_sheet_name)
            ws.set_tab_color(color='red')
            # # write each column to worksheet
            column_styles = []
            column_names = []
            for col_index, column in enumerate(mimic):
                # # set column width and append name and style to lists for use on write
                if self.formatting:
                    style = self.body_style
                    style.update({'num_format': column.simple_type.excel_format})
                else:
                    style = {'num_format': column.simple_type.excel_format}
                column_style = wb.add_format(style)
                column_styles.append(column_style)
                column_names.append(column.name)
                # width = self.__get_column_width(column=column)
                # ws.set_column(col_index, col_index, width)
                # # write header
                if self.formatting:
                    ws.write(0, col_index, column_names[col_index], header_style)
                else:
                    ws.write(0, col_index, column_names[col_index])
            for row_index, row in enumerate(mimic.rows()):
                for col_index, value in enumerate(row):
                    # # write column values
                    if not isinstance(value, basestring) and hasattr(value, '__iter__'):
                        value = str(value)
                    ws.write(row_index + 1, col_index, value, column_styles[col_index])
        # write sql
        if self.write_sql:
            self.__write_sql(mimic_list=mimic_list, workbook=wb)
        # clean up
        wb.close()

    def __write_sql(self, mimic_list, workbook):
        ws = workbook.add_worksheet(name='SQL')
        ws.set_tab_color(color='black')
        sql_list = []
        for index, mimic in enumerate(mimic_list):
            if mimic.name:
                name = mimic.name
            else:
                name = 'Data%s' % index
            sql = """%s:\n\n%s""" % (name, mimic.sql)
            sql = sql.split('\n')
            # make sure the first line has an entry
            for x in range(len(sql)):
                if not sql[0]:
                    del sql[0]
                else:
                    break
            sql_list.append(sql)
            width = self.__get_column_width(column=sql, vector=False)
            ws.set_column(index * 2, index * 2, width)
            if index > 0:
                ws.set_column((index * 2) - 1, (index * 2) - 1, 4)
        if len(mimic_list) == 1:
            for row_index, row in enumerate(sql_list[0]):
                ws.write(row_index, 0, row)
        else:
            write_sql = map(None, *sql_list)
            for row_index, row in enumerate(write_sql):
                for column_index, value in enumerate(row):
                    ws.write(row_index, column_index * 2, value)

    def __get_column_width(self, column, vector=True):
        try:
            # set column widths
            if vector:
                width = int(round((max([len(str(x)) for x in [column.name] + column if (x or x == 0)]) + 2.) * self.width_conversion))
            else:
                width = int(round((max([len(str(x)) for x in column if (x or x == 0)]) + 2.) * self.width_conversion))
        except (TypeError, ValueError):
            try:
                # if all column elements have no str method or are None columns will be sized off header
                width = int(round((len(column.name) + 2.) * self.width_conversion))
            except AttributeError:
                width = 8.43
        if width > self.max_column_width:
            width = self.max_column_width
        return width


class ExcelWriterV2(Writer):
    """
    Writes data_objects to excel.
    """

    font = 'Consolas'
    font_size = 11
    # width_conversion = font_size * 1.1
    width_conversion = 1.3
    header_style = {'font': font,
                    'size': font_size,
                    'bold': True,
                    'bottom': 1,
                    }
    body_style = {'font': font,
                  'size': font_size,
                  }

    constant_memory = True
    formatting = True
    max_column_width = 30

    write_sql = True

    def __init__(self):
        pass

    # TODO: write to row/col start locations.
    def write(self, data_list, file_name, sheet_name=None, row_start=0, col_start=0):
        # # set start indices
        if row_start:
            row_start -= 1
        if col_start:
            col_start -= 1
        # # open wb
        wb = xlsxwriter.Workbook(file_name, {'constant_memory': self.constant_memory})
        # # define header_style
        header_style = wb.add_format(self.header_style)
        # # write each mimic to its own sheet
        for i, obj in enumerate(data_list):
            # # get sheet name
            if not sheet_name and obj.name:
                this_sheet_name = obj.name
            else:
                this_sheet_name = 'Data%s' % i
            # # make sure name doesn't already exist in workbook (fatal error) ##
            this_sheet_name = make_names_unique(name_list=[ws.name for ws in wb.worksheets()]+[this_sheet_name],
                                                base_name=this_sheet_name, no_case=True)[-1]
            # # define worksheet
            ws = wb.add_worksheet(name=this_sheet_name)
            ws.set_tab_color(color='red')
            # # write each column to worksheet
            column_styles, column_names = [], []
            for col_index, column in enumerate(obj):
                # # set column width and append name and style to lists for use on write
                if self.formatting:
                    style = self.body_style
                    style.update({'num_format': column.simple_type.excel_format})
                else:
                    style = {'num_format': column.simple_type.excel_format}
                column_style = wb.add_format(style)
                column_styles.append(column_style)
                column_names.append(column.name)
                # width = self.__get_column_width(column=column)
                # ws.set_column(col_index, col_index, width)
                # # write header
                if self.formatting:
                    ws.write(row_start, col_index+col_start, column_names[col_index], header_style)
                else:
                    ws.write(row_start, col_index+col_start, column_names[col_index])
            for row_index, row in enumerate(obj.rows()):
                for col_index, value in enumerate(row):
                    # # write column values
                    if not isinstance(value, basestring) and hasattr(value, '__iter__'):
                        value = str(value)
                    ws.write(row_index+row_start+1, col_index+col_start, value, column_styles[col_index])
        # write sql
        if self.write_sql:
            self.__write_sql(data_list=data_list, workbook=wb)
        # clean up
        wb.close()

    def __write_sql(self, data_list, workbook):
        ws = workbook.add_worksheet(name='SQL')
        ws.set_tab_color(color='black')
        note_list = []
        for index, obj in enumerate(data_list):
            if obj.name:
                name = obj.name
            else:
                name = 'Data%s' % index
            note = """%s:\n\n%s""" % (name, obj.note)
            note = note.split('\n')
            # make sure the first line has an entry
            for x in range(len(note)):
                if not note[0]:
                    del note[0]
                else:
                    break
            note_list.append(note)
            width = self.__get_column_width(column=note, vector=False)
            ws.set_column(index * 2, index * 2, width)
            if index > 0:
                ws.set_column((index * 2) - 1, (index * 2) - 1, 4)
        if len(data_list) == 1:
            for row_index, row in enumerate(note_list[0]):
                ws.write(row_index, 0, row)
        else:
            write_note = map(None, *note_list)
            for row_index, row in enumerate(write_note):
                for column_index, value in enumerate(row):
                    ws.write(row_index, column_index * 2, value)

    def __get_column_width(self, column, vector=True):
        try:
            # set column widths
            if vector:
                width = int(round((max([len(str(x)) for x in [column.name] + column if (x or x == 0)]) + 2.) * self.width_conversion))
            else:
                width = int(round((max([len(str(x)) for x in column if (x or x == 0)]) + 2.) * self.width_conversion))
        except (TypeError, ValueError):
            try:
                # if all column elements have no str method or are None columns will be sized off header
                width = int(round((len(column.name) + 2.) * self.width_conversion))
            except AttributeError:
                width = 8.43
        if width > self.max_column_width:
            width = self.max_column_width
        return width


class CopyType(object):
    """
    A base type, can be combined with other similar data_objects.
    """
    def __add__(cls, other):
        """ Allows combining two type class data_objects. """
        if isinstance(other, cls):
            pass


class CopyValues(CopyType):
    pass


class CopyFormats(CopyType):
    pass


class CopyFormulas(CopyType):
    pass


class Copier(object):
    """
    I do copying things.
    """


class ExcelCopier(Copier):
    def __init__(self, from_file, to_file, from_range=(0, 0), to_range=(0, 0)):
        pass





