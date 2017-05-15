#!/usr/bin/python
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: SQLite virtual table vertica_log for vertica.log files on each nodes
# Author: DingQiang Liu

import db.vcluster as vcluster
import db.verticalog_filterdata as verticalog_filterdata


def create(vs):
  """ create and register virtual table vertica_log for vertica.log."""

  tableName = "vertica_log"
  # virtual table ddl
  ddl = """
    CREATE TABLE %s (
      time timestamp,
      node_name varchar(20),
      thread_name varchar(60),
      thread_id varchar(28),
      transaction_id integer,
      component varchar(30),
      level varchar(20),
      elevel varchar(20),
      enode varchar(20),
      message varchar(2322)
    );""" % tableName
  vs.ddls.update({tableName: ddl})

  # full text search local storage table ddl
  ddl4local = """
    CREATE VIRTUAL TABLE %s USING fts4(
      time timestamp,
      node_name varchar(20),
      thread_name varchar(60),
      thread_id varchar(28),
      transaction_id integer,
      component varchar(30),
      level varchar(20),
      elevel varchar(20),
      enode varchar(20),
      message varchar(2322),
      notindexed=time, 
      notindexed=node_name, 
      notindexed=thread_name, 
      notindexed=thread_id, 
      notindexed=transaction_id, 
      notindexed=component, 
      notindexed=level, 
      notindexed=elevel,
      notindexed=enode
    );""" % tableName
  vs.ddls4local.update({tableName: ddl4local})

  cursor = None 
  try :
    cursor = vs.connection.cursor()
    schemaname = ""
    if vs.connection.filename != "" :
      schemaname = "v_internal"
    
    cursor.execute("create virtual table %s using verticasource" % (tableName if schemaname == "" else schemaname+"."+tableName))
    vs.tables[tableName].remotefiltermodule = verticalog_filterdata

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
