import pyodbc
host = 'localhost'
port = 31010
uid = 'dremio'
pwd = 'dremio123'
driver = "/Library/Dremio/ODBC/lib/libdrillodbc_sbu.dylib"

cnxn=pyodbc.connect("Driver={};ConnectionType=Direct;HOST={};PORT={};AuthenticationType=Plain;UID={};PWD={}".format(driver,host,port,uid,pwd),autocommit=True)
