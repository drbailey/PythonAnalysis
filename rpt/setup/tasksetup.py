__author__  = 'drew bailey'
__version__ = 0.1

"""
Actions to perform on task initialization.
Builds default directories and files for tasks.
"""

from win32com import client
import os


def task_setup(task, user=''):
    """ New task setup. """
    ### make root folder ###
    if not os.path.exists(task):
        os.makedirs(task)
    os.chdir(task)

    ### make non-root folders ###
    folders = ['Parse',
               'Archives',
               'Data']
    for f in folders:
        if not os.path.exists(f):
            os.makedirs(f)

    ### default code text
    replacements = {'task': task,
                    'user': user}
    default_code = """

__author__ = '%(user)s'
__version__ = '1.0'

'''

'''

from rpt import RptMaster


### GENERAL ARGUMENTS ###
user_name = '%(user)s'
task_name = '%(task)s'
sub_task = ''
run_state = 'now'

### DATA PARAMETERS ###
source = ''
detail = '''

'''
variables = {
    'rundate_f': None,
    }
data_parameters = [
    (source, detail, variables),
    ]

### MERGE PARAMETERS ###
data_indices = []
how = 'inner'
on = [(), ]
merge_parameters = [
    (data_indices, how, on),
    ]

### PROCESS PARAMETERS ###
process_parameters = {}

### DISTRIBUTION PARAMETERS ###
# PATH #
# distro_path = ''

# EMAIL #
to = []
cc = []
bcc = []

files = []

# body_image = {
#     'row_start': 1,
#     'col_start': 1,
#     'row_end': None,
#     'col_end': None,
#     'height': 380,
#     'width': 800,
#     }

distribution_parameters = {
    # 'distro_path': distro_path,
    'to': to,
    'cc': cc,
    'bcc': bcc,
    # 'files': files,
    }

### MAIN ###
def main():
    r = RptMaster(
        user_name=user_name,
        task_name=task_name,
        sub_task=sub_task,
        run_state=run_state,
        data_parameters=data_parameters,
        merge_parameters=merge_parameters,
        process_parameters=process_parameters,
        distribution_parameters=distribution_parameters,
        )
    # r.get_data()
    # r.merge()
    # r.process()
    # r.distribute()
    r.run()

if __name__ == '__main__':
    main()

""" % replacements
    ### make text files ###
    files = [('config.ini', '[TASK]\n\n[DEFAULT]\n'),
             ('readme.txt', 'Readme for task %s:\n' % task),
             ('%s.py' % task, default_code)]
    for f in files:
        try:
            with open(f[0]):
                pass
        except:
            with open(f[0], 'w') as fo:
                fo.write(f[1])

    ### make com_std files ###
    fpath = os.getcwd()+'\Template_'+task+'.xlsx'
    try:
        excel = client.Dispatch("Excel.Application")
        WB = excel.Workbooks.Open(Filename=fpath, ReadOnly=1)
        WB.Close()
        excel.Quit()
        del excel
    except:
        try:
            excel = client.Dispatch("Excel.Application")
            #excel.Visible = 1
            WB = excel.Workbooks.Add()
            excel.Sheets(1).Name = task
            excel.Sheets(2).Name = 'Data0'
            excel.Sheets(3).Name = 'SQL'
            WB.SaveAs(fpath)
            WB.Close()
            excel.Quit()
            del excel
        except Exception, e:
            print e

    os.chdir('..')