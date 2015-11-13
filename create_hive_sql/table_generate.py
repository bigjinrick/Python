import mysql.connector

print '''
##########################
start to generate db ddl 
##########################
'''

dbConfig = {
    'user': 'select_only',
    'password': 'select_only',
    'host': '10.32.119.14',
    'database': 'hive',
    'use_unicode': True
}

# hiveDbs = ('oms', 'dop', 'prmt', 'csc', 'crm3rd'
# 	, 'emall_order', 'commodity', 'rigida', 'mrs', 'wxs'
# 	, 'crm', 'mdmuser', 'cxbdb', 'eadccc', 'grape20'
# 	, 'mdmpartner', 'society', 'ces_score_app', 'mms', 'mmstask'
# 	, 'sso', 'datacenter', 'pay_center', 'common_oms','carmall')

# hiveDbs = ("mdmuser", "oms", "dop", "prmt", "csc", "crm3rd", "emall_order", "commodity", "rigida", "mrs", "wxs", "crm", "cxbdb", "eadccc", "grape20"
#     , "mdmpartner", "ces_score_app", "mms", "mmstask", "sso", "pay_center", "common_oms")
hiveDbs = ("analysis",)

conn = mysql.connector.connect(**dbConfig)
cursor = conn.cursor()

scriptsDir = "scripts/"
createTableSqlTpl = "create table %s.%s (%s);"




for db in hiveDbs:
  # print secretTables
  scriptFileName = scriptsDir + db + "_ddl.sql"
  print "start to generate file: " + scriptFileName
  scriptFile = open(scriptFileName, "w")

  queryTablesSql = "select TBLS.TBL_NAME from DBS, TBLS WHERE DBS.DB_ID = TBLS.DB_ID AND DBS.NAME = %s"
  cursor.execute(queryTablesSql, (db,))
  tableList = cursor.fetchall()
  for (tableNameBytes,) in tableList:
    table_name = tableNameBytes.decode()

    queryTableColsSql = "select COLUMNS_V2.COLUMN_NAME,COLUMNS_V2.TYPE_NAME FROM DBS, TBLS, SDS, COLUMNS_V2 WHERE DBS.DB_ID = TBLS.DB_ID AND TBLS.SD_ID = SDS.SD_ID AND SDS.CD_ID = COLUMNS_V2.CD_ID AND DBS.NAME = %s and TBLS.TBL_NAME= %s order by COLUMNS_V2.INTEGER_IDX "
    cursor.execute(queryTableColsSql, (db, table_name))
    tableColList = cursor.fetchall()
    colNameTypes = ""
    for (tableColNameBytes,tableColTypeNameBytes) in tableColList:
      tableColName = tableColNameBytes.decode()
      tableColTypeName = tableColTypeNameBytes.decode()
      colNameTypes += tableColName + " " + tableColTypeName + ","
    createTableSql =  createTableSqlTpl % (db, table_name, colNameTypes[0: -1])

    
    print createTableSql
    scriptFile.write(createTableSql + "\n")
  scriptFile.close()
  print "end to generate file: " + scriptFileName

cursor.close()
conn.close()
