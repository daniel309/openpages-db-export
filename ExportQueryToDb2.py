#!/usr/bin/env python
# coding: utf-8

# In[69]:


# @hidden_cell
# The following code contains credentials. You might want to remove 
# those credentials before you share this notebook.




import os, types
from ibm_watson_studio_lib import access_project_or_space
from timeit import default_timer as timer

# ##############################################
# Configure environment with job variables, e.g.
# QUERY=select+*+from+%5BSOXProcess%5D
# TABLE=SOXProcess
# ##############################################

if os.environ.get('TABLE') == None: 
    os.environ['TABLE'] = 'OPDEFAULTTAB'
    
if os.environ.get('QUERY') == None: 
    os.environ['QUERY'] = 'select+*+from+%5BSOXProcess%5D'

wslib = access_project_or_space()
db2_metadata = wslib.get_connection("ZDataDb2LUW")


# customize here
class Configuration:
    op_host = 'https://op-bct-fyre511.fyre.ibm.com:10111'
    op_api_base = op_host + '/grc/api/'
    op_user = 'opuser'
    op_pwd = '******' #SECRET!
    op_query = 'query?q=' + os.environ['QUERY']
    op_query_pagesize = '30000'
    db2_table_schema = 'DMARTIN'
    db2_table_name = os.environ['TABLE']
    db2_database = db2_metadata['database']
    db2_host = db2_metadata['host']
    db2_port = db2_metadata['port']
    db2_user = db2_metadata['username']
    db2_pwd = db2_metadata['password']
args=Configuration()

JSONtoSQLTypeMapper = {
    'ENUM_TYPE': 'VARCHAR(200), ',
    'MULTI_VALUE_ENUM': 'VARCHAR(200), ',
    'DATE_TYPE': 'TIMESTAMP, ',
    #'DATE_TYPE': 'VARCHAR(30), ',
    'BOOLEAN_TYPE': 'INT, ',
    'INTEGER_TYPE': 'BIGINT, ',
    'ID_TYPE': 'BIGINT, ',
    'STRING_TYPE': 'VARCHAR(800), ',
}


# In[52]:


import os, ibm_db, ibm_db_dbi as dbi

def createDb2Connection():
    dsn = 'DATABASE={};HOSTNAME={};PORT={};PROTOCOL=TCPIP;UID={uid};PWD={pwd}'.format(
        args.db2_database,
        args.db2_host,
        args.db2_port,
        uid = args.db2_user,
        pwd = args.db2_pwd
    )

    display("Connecting to Db2...")
    conn = ibm_db.pconnect(dsn,"","") #persistent, reusable connection
    display("Connected!")
    return conn

def prepareInsertStatement(conn, numcolumns):
    mark = '?,' * numcolumns
    sql = "INSERT INTO \"{schema}\".\"{tabname}_SHADOW\" VALUES ({markers})".format(
        schema = args.db2_table_schema, 
        tabname = args.db2_table_name, 
        markers = mark[:-1])
    display(sql)
    prep = ibm_db.prepare(conn, sql)
    if prep is False: raise("ERROR: Unable to prepare the SQL statement specified.")
    return prep

def runInsertBatch(stmt, tuple_of_tuples):
    # http://htmlpreview.github.io/?https://github.com/IBM/db2-python/blob/master/HTML_Documentation/ibm_db-execute_many.html
    #display(tuple_of_tuples)
    start = timer()
    ret = ibm_db.execute_many(stmt, tuple_of_tuples)
    end = timer()
    if ret is True: raise("ERROR: Unable to execute the SQL statement specified.\n")
    display("Batch-Inserted {} rows in {:.2f}s".format(len(tuple_of_tuples), end-start))

def runInsertSingle(stmt, tuple_of_tuples):
    start = timer()
    for t in tuple_of_tuples:
        try:
            ret = ibm_db.execute(stmt, t)
        except:
            display("Insert ERROR: stmt={}, tuple={}".format(stmt, t))
            raise
    end = timer()
    display("Singleton-Inserted {} rows in {:.2f}s".format(len(tuple_of_tuples), end-start))
    
def flipTableVersions(conn):
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_OFF)
    try:
        ibm_db.exec_immediate(conn, generateDropTableSQL())
        ibm_db.exec_immediate(conn, generateRenameTableSQL())
        ibm_db.commit(conn)
    except:
        ibm_db.rollback(conn)
    ibm_db.autocommit(conn, ibm_db.SQL_AUTOCOMMIT_ON)
    
def runCreateTableSQL(conn, sql):
    display(sql)
    try:
        ibm_db.exec_immediate(conn, generateDropTableSQLForShadow())
    except Exception:
        pass
    ibm_db.exec_immediate(conn, sql)
    
def generateCreateSQLPrefix():
    return "CREATE TABLE \"{schema}\".\"{tabname}_SHADOW\" (".format(
        schema = args.db2_table_schema, 
        tabname = args.db2_table_name)

def generateRenameTableSQL():
    return "RENAME TABLE \"{schema}\".\"{tabname}_SHADOW\" TO \"{tabname1}\"".format(
        schema = args.db2_table_schema, 
        tabname = args.db2_table_name, 
        tabname1 = args.db2_table_name)

def generateDropTableSQL():
    return "DROP TABLE \"{schema}\".\"{tabname}\"".format(
        schema = args.db2_table_schema, 
        tabname = args.db2_table_name)

def generateDropTableSQLForShadow():
    return "DROP TABLE \"{schema}\".\"{tabname}_SHADOW\"".format(
        schema = args.db2_table_schema, 
        tabname = args.db2_table_name)

def generateSQLColumnFromJSON(field):
        return '\"' + field['id'] + field['name'].replace(' ', '') + '\" ' + JSONtoSQLTypeMapper.get(field['dataType'], '###UNKNOWN TYPE###') 
        
def closeCreateSQL(sql):
    return sql[:-2] + ')'

def generateCreateTableSQL(row):
    sql = generateCreateSQLPrefix()
    fields = row.get('fields').get('field')
    for field in fields:
        sql += generateSQLColumnFromJSON(field)
    sql = closeCreateSQL(sql)
    return sql


# In[64]:


import requests
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def runRESTQuery(query):
    if query == None: return None
    query += '&pageSize=' + args.op_query_pagesize
    start = timer()
    r = requests.get(args.op_api_base + query, auth=(args.op_user, args.op_pwd), verify=False) #skips cert validation
    end = timer()
    display("Query ran {:.2f}s -- {}".format(end-start, query))
    if r.status_code == requests.codes.ok:
        s = timer()
        res = r.json()
        e = timer()
        display("Parsed JSON in {:.2f}s".format(e-s))
        return res
    else:
        r.raise_for_status()
        return None

def nextResultsLink(root):
    if root == None: 
        return None
    for link in root.get("links"):
        if link.get('rel') == 'next':
            return link.get('href')
    return None

def extractFieldsIntoTuple(fields):
    t  = []
    value = None
    for field in fields:
        if field.get('value') is not None:
            if field['dataType'] == 'DATE_TYPE': 
                value = field['value'][:field['value'].rfind('-')] # strip tz offset
            elif field['dataType'] == 'STRING_TYPE':
                #value = '##string##' + field['value']
                value = field['value']
            else:
                value = field['value']
        elif field.get('enumValue') is not None: 
            #value = '##enum##' + field['enumValue']['name']
            value = field['enumValue']['name']
        elif field.get('multiEnumValue') is not None: 
            #value = '##multi-enum##' + ','.join(v['name'] for v in field['multiEnumValue']['enumValue'])
            value = ','.join(v['name'] for v in field['multiEnumValue']['enumValue'])
        else: 
            value = None

        t.append(value)
    return tuple(t)

def returnFirstField(root):
    return root.get("rows")[0].get('fields').get('field')

def run():
    root = runRESTQuery(args.op_query)
    
    sql = generateCreateTableSQL(root.get("rows")[0])
    conn = createDb2Connection()
    runCreateTableSQL(conn, sql)
    stmt = prepareInsertStatement(conn, len(returnFirstField(root)))
    
    while root is not None:
        tuples = []
        for row in root.get("rows"):
            fields = row.get('fields').get('field')
            t = extractFieldsIntoTuple(fields)
            tuples.append(t)
        
        #runInsertSingle(stmt, tuple(tuples))
        runInsertBatch(stmt, tuple(tuples))
        root = runRESTQuery(nextResultsLink(root))
        
    flipTableVersions(conn)
    display("Done!")


# In[70]:


run()


# In[4]:




