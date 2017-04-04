#!/usr/bin/python
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: SQLite virtual table dblog for Vertica dbLog files on each nodes
# Author: DingQiang Liu

import db.vcluster as vcluster
import db.vdblog_filterdata as vdblog_filterdata


def create(vs):
  """ create and register virtual table dblog for Vertica dbLog."""

  tableName = "dblog"
  ddl = """
    CREATE TABLE %s (
      time timestamp,
      node_name varchar(20),
      component varchar(30),
      message varchar(2322)
    );""" % tableName
  vs.ddls.update({tableName: ddl})

  cursor = None 
  try :
    cursor = vs.connection.cursor()
    schemaname = ""
    if vs.connection.filename != "" :
      schemaname = "v_internal"
    
    cursor.execute("create virtual table %s using verticasource" % (tableName if schemaname == "" else schemaname+"."+tableName))
    vs.tables[tableName].remotefiltermodule = vdblog_filterdata

    columns = [ columnName.lower() for _, columnName, _, _, _, _ in cursor.execute("pragma table_info('%s');" % tableName) ]
    columns.insert(0, u"rowid")
    vs.tables[tableName].columns = columns        
    # only keep the first part of SQL typename
    vs.tables[tableName].columnTypes = { columnName.lower(): columnType.split(' ')[0].split('(')[0].lower() for _, columnName, columnType, _, _, _ in cursor.execute("pragma table_info('%s');" % tableName) }
    vs.tables[tableName].columnTypes[u"rowid"] = "integer"
    # primary keys
    vs.tables[tableName].primaryKeys = None
  finally :
    if not cursor is None :
      cursor.close();
