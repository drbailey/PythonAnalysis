__author__  = 'drew bailey'
__version__ = 0.1

"""
win32com related methods for use in other programs. Intended to replace Visual Basic in current reporting where
possible.
"""

from win32com import client
import os
import re


# ## defaults ###

# alter this based on windows version
INDEXING = 1


# ## com data_objects ###

def make_app(app_string, visible=False):
    """
    Will include all com apps.
    :param app_string:
    :return:
    """
    if app_string.lower() == 'excel':
        app = client.Dispatch("Excel.Application")
    else:
        return
    if visible:
        app.Visible = True
    return app


def deactivate(app):
    """
    Drops warnings and alerts from an excel application instance.
    :param app: a win32com excel application instance.
    :return:
    """
    try:
        app.DisplayAlerts = False
    except:
        pass
    try:
        app.AskToUpdateLinks = False
    except:
        pass
    try:
        app.AlertBeforeOverwriting = False
    except:
        pass
    try:
        app.EnableEvents = False
    except:
        pass
    try:
        app.ScreenUpdating = False
    except:
        pass


def reactivate(app):
    """
    Re-enables warnings and alerts from an excel application instance.
    :param app: a win32com excel application instance.
    :return:
    """
    try:
        app.DisplayAlerts = True
    except:
        pass
    try:
        app.AskToUpdateLinks = True
    except:
        pass
    try:
        app.AlertBeforeOverwriting = True
    except:
        pass
    try:
        app.EnableEvents = True
    except:
        pass
    try:
        app.ScreenUpdating = True
    except:
        pass


def last_row(worksheet, col=1):
    """
    Finds the last used row in a column.
    :param worksheet: win32com worksheet object
    :param col: Column letter to check.
    :return: Last row as integer or long.
    """
    # last_r = ws.Range(col + str(ws.Rows.Count)).End('xlUp').Row
    # last_r = worksheet.Range(col + str(worksheet.Rows.Count)).End(-4162).Row
    last_r = worksheet.Cells(worksheet.Rows.Count, col).End(-4162).Row
    return last_r


def last_column(worksheet, row=INDEXING):
    """
    Finds the last used column in a row.
    :param worksheet: win32com worksheet object.
    :param row: Row index to check.
    :return: Last used column as integer or long.
    """
    last_c = worksheet.Cells(row, worksheet.Columns.Count).End(-4159).Column
    return last_c


def open_workbook(app, file_name, path=None):
    if not path:
        path = os.getcwd()
    wb = app.Workbooks.Open(Filename=os.path.join(path, file_name), ReadOnly=0)
    return wb


def get_sheet_names(workbook):
    """ Gets all sheet names in a workbook object. """
    return [sheet.Name for sheet in workbook.Sheets]


def get_sheets(workbook):
    """ Gets all sheet data_objects in a workbook object. """
    return [sheet for sheet in workbook.Sheets]


def get_sheet(app, sheet):
    """ Gets worksheet object from index or name. """
    return app.Worksheets(sheet)


def drop_app(app):
    reactivate(app=app)
    app.Quit()
    del app


def __str_to_ind(label):
    l = []
    char_limit = 26
    for i, char in enumerate(label[::-1].lower()):
        char_val = ord(char)-96
        if char_val > 26:
            raise ValueError('%s is not an allowed column character.' % label[i])
        if i < 1:
            l.append(char_val)
            continue
        total_val = (char_limit**i)*char_val
        #  print 'Character %s; becomes value %s, and totals to %s for index %s.' % (char, char_val, total_val, i)
        l.append(total_val)
    grand_total = sum(l)-1
    grand_total += INDEXING
    #  print '%s becomes %s and is converted to %s.' % (label, label[::-1].lower(), grand_total)
    return grand_total


def __ind_to_str(label):
    characters = []
    char_limit = 26
    label = int(label)
    label += 1
    i = 0
    while True:
        if char_limit**i > label:
            break
        i += 1
    i -= 1
    while i >= 0:
        remainder = label % (char_limit**i)
        characters.append(chr(label//(char_limit**i)+96).upper())
        label = remainder
        i -= 1
    return ''.join(characters)
    

def convert_col_label(column_label):
    """
    Converts an excel name to an index or vice versa.
    """
    try:
        column_label = int(column_label)
        grand_total = __ind_to_str(column_label)
    except ValueError:
        grand_total = __str_to_ind(label=column_label)
    return grand_total


def parse_range(range_string):
    """
    Turns excel ranges into number ranges.
    :param range_string: Range as excel range string. ex) 'A4:B9'
    :return:
    """
    if re.search(':', range_string):
        m = re.search('(\D+)(\d+):(\D+)(\d+)', range_string)
        if m:
            return m.groups()
        else:
            print 'Could not parse range string %s.' % range_string
            return None, None, None, None
    else:
        m = re.search('(\D+)(\d+)', range_string)
        if m:
            return m.groups()
        else:
            print 'Could not parse range string %s.' % range_string
            return None, None


def get_max_range(worksheet, ck_row_s=None):
    if not ck_row_s:
        ck_row_s = INDEXING
    columns = last_column(worksheet=worksheet, row=ck_row_s)
    max_rows = INDEXING
    for column in range(INDEXING, columns + INDEXING):
        # column = convert_col_label(column)
        max_row = last_row(worksheet=worksheet, col=column)
        if max_rows < max_row:
            max_rows = max_row
    return columns, max_rows


def get_range(worksheet, row_start, col_start, row_end=None, col_end=None, hidden=True):
    """

    :param worksheet:
    :param row_start:
    :param col_start:
    :param row_end:
    :param col_end:
    :param hidden: Only accept visible cells. xlCellTypeVisible = 12
    :return:
    """
    if not col_end and not row_end:
        col_end, row_end = get_max_range(worksheet=worksheet)
    elif not col_end:
        col_end = last_column(worksheet=worksheet)
    elif not row_end:
        row_end = last_row(worksheet=worksheet)
    row_start = int(row_start)
    row_end = int(row_end)
    try:
        col_start = int(col_start)
    except ValueError:
        col_start = convert_col_label(column_label=col_start)
    try:
        col_end = int(col_end)
    except ValueError:
        col_end = convert_col_label(column_label=col_end)

    rng = worksheet.Range(worksheet.Cells(row_start, col_start), worksheet.Cells(row_end, col_end))
    if not hidden:
        rng = rng.SpecialCells(12)
    return rng


def copy_range(from_worksheet, to_worksheet, from_row=INDEXING, from_col=INDEXING, to_row=INDEXING, to_col=INDEXING,
               from_row_end=None, from_col_end=None, paste=-4163):
    """

    :param from_worksheet:
    :param to_worksheet:
    :param from_row:
    :param from_col:
    :param to_row:
    :param to_col:
    :param from_row_end:
    :param from_col_end:
    :param paste: -4163 xlPasteValues
    :return:
    """
    from_rng = get_range(row_start=from_row, col_start=from_col, row_end=from_row_end, col_end=from_col_end,
                         worksheet=from_worksheet)
    to_rng = to_worksheet.Cells(to_row, to_col)
    from_rng.Copy()
    to_rng.PasteSpecial(Paste=paste)
    

def save_workbook(workbook, save_name=None, path=None):
    if not path:
        path = os.getcwd()
    if save_name:
        workbook.SaveAs(os.path.join(path, save_name))
    else:
        workbook.Save()
    return workbook


def unhide(workbook):
    l = []
    for sheet in get_sheet_names(workbook=workbook):
        ws = workbook.Sheets(sheet)
        if ws.Visible == 0:
            ws.Visible = -1
            l.append(sheet)
    return l


def hide(workbook, names):
    sheets = get_sheet_names(workbook=workbook)
    if sheets == names:
        names = names[:-1]
    if isinstance(names, basestring):
        names = [names]
    for name in names:
        ws = workbook.Sheets(name)
        if ws.Visible != 0:
            ws.Visible = 0


def close_workbook(workbook, save=False):
    workbook.Close(save)


def auto_fit_columns(worksheet):
    worksheet.Cells.EntireColumn.AutoFit()


def collapse_pivots(worksheet):
    for table in worksheet.PivotTables():
        for field in table.PivotFields():
            try:
                field.ShowDetail = False
            except:
                pass


def range_to_image(worksheet, range, height=None, width=None, filename=None):
    """
    Saves excel range "range" to an image file named "filename" of height and width "height" and "width" using
        worksheet named "worksheet" as an intermediate working sheet.
    :param worksheet:
    :param range:
    :param height:
    :param width:
    :param filename:
    :return:
    """
    xlBitmap = 2  # Bitmap (.bmp, .jpg, .gif).
    xlPicture = -4147  # Drawn picture (.png, .wmf, .mix).
    xlScreen = 1  # The picture is copied as it will look when it is printed.
    xlPrinter = 2  # The picture is copied to resemble its display on the screen as closely as possible.

    chart = worksheet.Shapes.AddChart()
    if height:
        chart.Height = height
    if width:
        chart.Width = width
    range.CopyPicture(xlPrinter, xlPicture)
    chart.Chart.Paste()
    if filename:
        if u'\\' not in filename:  # TODO: fix this check to use a generic separator.
            filename = os.path.join(unicode(os.getcwd()), filename)
    else:
        filename = os.path.join(unicode(os.getcwd()), 'image.png')
    chart.Chart.Export(filename)
    chart.Delete()


def default_range_to_image(workbook_name, worksheet_name, row_start, col_start, row_end, col_end,
                           image_name=u'image.png', height=None, width=None):
    """
    Saves an image of an excel range.
    :param workbook_name:
    :param worksheet_name:
    :param row_start:
    :param col_start:
    :param row_end:
    :param col_end:
    :param image_name:
    :param height:
    :param width:
    :return:
    """
    # open app #
    app = client.Dispatch("Excel.Application")
    deactivate(app)
    # open workbook #
    wb = open_workbook(file_name=workbook_name, app=app)
    # open worksheet #
    ws = get_sheet(app=app, sheet=worksheet_name)
    # get range #
    rng = get_range(worksheet=ws, row_start=row_start, col_start=col_start, row_end=row_end, col_end=col_end)
    # make a new worksheet to #
    temp_ws = wb.Sheets.Add()
    # save range to image #
    range_to_image(worksheet=temp_ws, range=rng, height=height, width=width, filename=image_name)
    temp_ws.Delete()
    # clean up #
    close_workbook(workbook=wb, save=False)
    reactivate(app)
    drop_app(app=app)


def default_copy(from_file_name, to_file_name, save_file_name=None, header=True, opacity=4, locations=None, collapse=True):
    """
    Handles rpt package default copy method. Medium level method.
    :param from_file_name: 
    :param to_file_name: 
    :param save_file_name:
    :param header:
    :param opacity:
    :param locations: dictionary of {sheet_name_or_index: (to_sheet_name, start_row, start_col), ...}
    
    opacity concept:
        0:' unhide all sheets, nothing at all hidden (or completely translucent... haha, physics is useful in programming)
        1:' unhide all sheets, sheets unhidden are rehidden
        2:' unhide all sheets, sheets unhidden are rehidden and 'SQL' named tabs copied to are hidden
        3:' unhide all sheets, sheets unhidden are rehidden and 'SQL' and 'data*' named tabs copied to are hidden
        4:' unhide all sheets, sheets unhidden are rehidden and all sheets where copy occurs is hidden
    """
    if header:
        from_row = INDEXING
    else:
        from_row = INDEXING + 1
    if not locations:
        locations = {}

    # open app #
    app = client.Dispatch("Excel.Application")
    deactivate(app)

    # open workbooks #
    from_wb = open_workbook(file_name=from_file_name, app=app)
    to_wb = open_workbook(file_name=to_file_name, app=app)

    # un-hide currently hidden sheets #
    to_hidden = unhide(workbook=to_wb)  # ERROR HERE
    unhide(workbook=from_wb)  # list not stored since from file is not saved

    # get sheet names #
    from_sheets = get_sheet_names(workbook=from_wb)
    to_sheets = get_sheet_names(workbook=to_wb)

    # make copies #
    for i, sheet in enumerate(from_sheets):
        from_ws = from_wb.Sheets(sheet)

        # hide sheets
        if opacity == 2:
            if 'sql' in sheet.lower():
                to_hidden.append(sheet)
        elif opacity == 3:
            if any([x for x in ['data', 'sql'] if x in sheet.lower()]):
                to_hidden.append(sheet)
        elif opacity > 3:
            to_hidden.append(sheet)

        # get paste location
        to_sheet, to_row, to_col = locations.get(i, (None, None, None))
        if to_row is None or to_col is None:
            to_sheet, to_row, to_col = locations.get(sheet, (None, INDEXING, INDEXING))
        else:
            to_row += (INDEXING - 1)
            to_col += (INDEXING - 1)
        if to_sheet:
            sheet = to_sheet

        # make sheets
        if sheet.lower() in [x.lower() for x in to_sheets]:
            to_ws = to_wb.Sheets(sheet)
        else:
            to_ws = to_wb.Sheets.Add()
            to_ws.Name = sheet

        # copy range
        copy_range(from_worksheet=from_ws, to_worksheet=to_ws, from_row=from_row, to_row=to_row, to_col=to_col)

    # refresh workbook data #
    to_wb.RefreshAll()

    # collapse all pivots #
    if collapse:
        for sheet in to_sheets:
            collapse_pivots(worksheet=to_wb.Sheets(sheet))

    # re-hide sheets #
    if opacity:
        hide(workbook=to_wb, names=to_hidden)

    # save, close workbooks, clean up app #
    if save_file_name:
        to_wb = save_workbook(workbook=to_wb, save_name=save_file_name)
        close_workbook(workbook=to_wb, save=False)
    else:
        close_workbook(workbook=to_wb, save=True)
    close_workbook(workbook=from_wb, save=False)
    reactivate(app)
    drop_app(app=app)


