import rpt

# library tests
user_name = 'drew'
lib = rpt.Library(user_name=user_name)

db_name = 'cms_test'  # preloaded
lib.connect(ctype=db_name)

srv = lib[-1]
table_name = 'cntrdef'
schema = 'fccms'
srv.select(table_name=table_name, schema=schema)

tbl = srv[-1]

fld = tbl['VALIDUNTIL']
fld.sort()


# example sql #
sql = """
select *
from fccms.cntrdef
;"""
