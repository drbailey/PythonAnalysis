import rpt

# pyodbc
# xlsxwriter
# xlrd
# python-dateutil
# download win32com package from sourceforge

example_data = [
    (1,2,3),
    (4,5,6),
    ]
t0 = rpt.Table(example_data)
print t0.__repr__()
print t0

t1 = rpt.Table(example_data, header=['a','b','c'])
print t1.__repr__()
t1.name = 'example data'
print t1.__repr__()
print t1

##t2 = rpt.Table(example_data)

f1 = rpt.Field([3.5, 6.5], name='d')
print f1.__repr__()
print f1

t1.append(f1)
print t1
##t1.to_csv()
print t1.note
##t1.to_excel()

sm = rpt.SetupMaster()
sm.menu()

