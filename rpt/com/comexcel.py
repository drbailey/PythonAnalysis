__author__  = 'drew bailey'
__version__ = 0.1

"""
THIS IS NOT IN USE.
Excel handling using win32com. Package intended to run many operations on an object after initialization.
Largely deprecated by openpyxl and comappmethods methods, however still used to run workbook VBA code.
"""

from .comappmethods import drop_app
from win32com import client
import os


class ComExcel():
    """
    ComExcel handles windows related interactions.

    task_name = Always passed as first argument, usually a PATH change in a master template.
    module   = VBA module name if applicable, defaults to 'DataProcessor'.
    macro    = VBA macro name if applicable, defaults to 'Processor'.
    template = Full PATH to VBA to execute.
    
    """
    def __init__(self, task_name='', rundate=None, sub_task=None, module='DataProcessor', macro='Processor',
                 template=r'MasterTemplate.xlsm', path=None):
        self.rundate = rundate
        self.module = module
        self.macro = macro
        self.task_name = task_name
        self.sub_task = sub_task
        self.template = template
        if not path:
            self.path = os.getcwd()+'\\'+task_name+'\\'
        else:
            self.path = path
        self.fullpath = self.path+'\\'+self.template

    def call_VBA(self, *args):
        """
        Runs VBA code. Supports up to 5 arguments currently.
        Default Master Processor arguments are (datalim, sub_task)
        task_name and date are always sent.
        """
        task_name = self.task_name
        sub_task = self.sub_task
        rundate = self.rundate
        module = self.module
        macro = self.macro
        # Place optional sub_task argument last.
        if args:
            args = list(args)
            if sub_task:
                args.append(sub_task)
        else:
            if sub_task:
                args = [sub_task]
        n = len(args)
        # print task_name, rundate, args, n
        # print n, [arg for arg in args]
        mpath = self.fullpath
        # print mpath
        try:
            excel = client.Dispatch("Excel.Application")
            excel.Visible = 1
            excel.Workbooks.Open(Filename=mpath, ReadOnly=1)
            try:
                if n == 0:
                    excel.Application.Run("%s!%s.%s" % (self.template, module, macro), task_name, rundate)
                elif n == 1:
                    excel.Application.Run("%s!%s.%s" % (self.template, module, macro), task_name, rundate, args[0])
                elif n == 2:
                    excel.Application.Run("%s!%s.%s" % (self.template, module, macro), task_name, rundate, args[0],
                                          args[1])
                elif n == 3:
                    excel.Application.Run("%s!%s.%s" % (self.template, module, macro), task_name, rundate, args[0],
                                          args[1], args[2])
                elif n == 4:
                    excel.Application.Run("%s!%s.%s" % (self.template, module, macro), task_name, rundate, args[0],
                                          args[1], args[2], args[3])
                elif n == 5:
                    excel.Application.Run("%s!%s.%s" % (self.template, module, macro), task_name, rundate, args[0],
                                          args[1], args[2], args[3], args[4])
                else:
                    print 'n is expected to be an integer between 0 and 4, (5 if no sub_task), check your arguments.'
            except:
                if n == 0:
                    excel.Application.Run("%s!%s" % (self.template, macro), task_name, rundate)
                elif n == 1:
                    excel.Application.Run("%s!%s" % (self.template, macro), task_name, rundate, args[0])
                elif n == 2:
                    excel.Application.Run("%s!%s" % (self.template, macro), task_name, rundate, args[0], args[1])
                elif n == 3:
                    excel.Application.Run("%s!%s" % (self.template, macro), task_name, rundate, args[0], args[1],
                                          args[2])
                elif n == 4:
                    excel.Application.Run("%s!%s" % (self.template, macro), task_name, rundate, args[0], args[1],
                                          args[2], args[3])
                elif n == 5:
                    excel.Application.Run("%s!%s" % (self.template, macro), task_name, rundate, args[0], args[1],
                                          args[2], args[3], args[4])
                else:
                    print 'n is expected to be an integer between 0 and 4, (5 if no sub_task), check your arguments.'
        except Exception, e:
            print 'Error: %s.' % e
            # onError()
        finally:
            try:
                drop_app(excel)
            except:
                pass
            
    def call_VBA_plain(self, debug=False):
        """ Template-independent VBA run, no arguments allowed. """
        if debug:
            print self.module, self.macro, self.template, self.fullpath
        excel = client.Dispatch("Excel.Application")
        excel.Workbooks.Open(Filename=self.fullpath, ReadOnly=0)
        try:
            excel.Application.Run("'%s'!%s.%s" % (self.template, self.module, self.macro))
        except:
            excel.Application.Run("'%s'!%s" % (self.template, self.macro))
        finally:
            drop_app(excel)

### MIMIC IMPORT LOOP, THIS IS STOPPED FOR NOW
    # def read_range(self, xlrange, sheet=1, filepath=None, header=False):
    #     """
    #     reads excel range into mimic data object
    #     :param xlrange: target range ex) 'A2:B4'
    #     :param sheet: target sheet name or index
    #     :param filepath: target PATH
    #     :param header: Boolean, if header is included in range
    #     :return: mimic object of range data
    #     """
    #     from ..util import WorkingTable
    #     if not filepath: filepath = '%s\\%s %s.xlsx' % (self.task_name, self.task_name, self.rundate)
    #     excel = client.Dispatch("Excel.Application")
    #     #excel.Visible = 1 #  comment this out later
    #     wb = excel.Workbooks.Open(Filename=filepath, ReadOnly=1)
    #     ws = excel.Sheets(sheet)
    #     rng = ws.Range(xlrange)
    #     data = rng.Item()
    #     if header:
    #         m = WorkingTable(data=data[1:], header=data[0])
    #     else:
    #         m = WorkingTable(data=data)
    #     wb.SaveAs(filepath)
    #     try:
    #         clean_up_app(excel)
    #     except:
    #         pass
    #     return m

    def write_range(self, data, start=(1, 1), sheet=1, filepath=None, header=False):
        """
        writes a mimic to an excel range
        :param data: mimic data object
        :param start: (row, column) index position for start
        :param sheet: target sheet
        :param filepath: target file PATH
        :param header: Boolean, if header is included
        :return: None
        """
        if not filepath: filepath = '%s\\%s %s.xlsx' % (self.task_name, self.task_name, self.rundate)
        excel = client.Dispatch("Excel.Application")
        #excel.Visible = 1 #  comment this out later
        try:
            wb = excel.Workbooks.Open(Filename=filepath, ReadOnly=0)
        except:
            wb = excel.Workbooks.Add()
        ws = excel.Sheets(sheet)
        rows, columns = data.dimensions()
        if header:
            ws.Range(ws.Cells(start[0], start[1]), ws.Cells(start[0], start[1] + columns - 1)).Value = data.header
            ws.Range(ws.Cells(start[0] + 1, start[1]), ws.Cells(start[0] + rows, start[1] + columns - 1)).Value = data
        else:
            ws.Range(ws.Cells(start[0], start[1]), ws.Cells(start[0] + rows - 1, start[1] + columns - 1)).Value = data
        wb.SaveAs(filepath)
        try:
            clean_up_app(excel)
        except:
            pass


































##### STUFF FOR LATER
##
##        fpath = os.getcwd()+'\Template_'+task+'.xlsx'
##    try:
##        excel.Workbooks.Open(Filename=fpath,ReadOnly=1)
##        excel.Quit()
##        del(excel)
##    except:
##        try:
##            excel = client.Dispatch("Excel.Application")
##            excel.Visible = 1
##
##            WB = excel.Workbooks.Add()
##            excel.Sheets(1).Name = task
##            excel.Sheets(2).Name = 'Data'
##            excel.Sheets(3).Name = 'SQL'
##            WB.SaveAs(fpath)
##            WB.Close()
##            excel.Quit()
##        except Exception,e:
##            print e
##### STUFF THAT WORKS ###
##
##excel.Cells(1,1).Value = 'Testing...'
##R = excel.Range('A1:C2')
##R.Item()
##
##WS = excel.WB.Worksheets.Add()
##excel.WS.Name = '%s'%task
##WS = excel.WB.Worksheets.Add(After:=.Sheets(.Sheets.Count))
##excel.WS.Name = 'Data'
##del(WS)
##del(WB)
##del(excel)
