#!/usr/bin/python
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: SQLite virtual tables for Vertica data collectors
# Author: DingQiang Liu

import db.vcluster as vcluster
import db.vdatacollectors_filterdata as vdatacollectors_filterdata


def create(vs):
  """ create and register virtual tables for Vertica datacollectors."""

  vc = vcluster.getVerticaCluster()
  ddls = vc.executors[0].remote_exec(getDDLs, catalogpath=vc.catPath).receive()

  #TODO: debug dc_requests_issued
  #ddls = {"dc_requests_issued": ddls["dc_requests_issued"]}

  vs.ddls.update(ddls)

  cursor = None 
  try :
    cursor = vs.connection.cursor()
    schemaname = ""
    if vs.connection.filename != "" :
      schemaname = "v_internal"

    for tableName in ddls :
      cursor.execute("create virtual table %s using verticasource" % (tableName if schemaname == "" else schemaname+"."+tableName))
      vs.tables[tableName].remotefiltermodule = vdatacollectors_filterdata

    for tableName in ddls :
      columns = [ columnName.lower() for _, columnName, _, _, _, _ in cursor.execute("pragma table_info('%s');" % tableName) ]
      columns.insert(0, u"rowid")
      vs.tables[tableName].columns = columns        
      # only keep the first part of SQL typename
      vs.tables[tableName].columnTypes = { columnName.lower(): columnType.split(' ')[0].split('(')[0].lower() for _, columnName, columnType, _, _, _ in cursor.execute("pragma table_info('%s');" % tableName) }
      vs.tables[tableName].columnTypes[u"rowid"] = "integer"
      # primary keys
      primaryKeys = ["time", "node_name"]
      if "transaction_id" in columns and "statement_id" in columns  and "path_id" in columns :
        primaryKeys.append("transaction_id")
        primaryKeys.append("statement_id")
        primaryKeys.append("path_id")
      elif "transaction_id" in columns and "statement_id" in columns  and "projection_oid" in columns :
        primaryKeys.append("transaction_id")
        primaryKeys.append("statement_id")
        primaryKeys.append("projection_oid")
      elif "transaction_id" in columns and "statement_id" in columns  and "sip_expr_id" in columns :
        primaryKeys.append("transaction_id")
        primaryKeys.append("statement_id")
        primaryKeys.append("sip_expr_id")
      elif "transaction_id" in columns and "statement_id" in columns :
        primaryKeys.append("transaction_id")
        primaryKeys.append("statement_id")
      else :
          for col in {"transaction_id", "txn_id", "session_id", "pool", "pool_name", "oid", "start_commit_id", "commit_epoch", "event_name", "device_name", "interface_id", "remote_node_name", "token_rounds", "path", "feature"} :
            if col in columns :
              primaryKeys.append(col)
              break

      vs.tables[tableName].primaryKeys = primaryKeys
  finally :
    if not cursor is None :
      cursor.close();



def getDDLs(channel, catalogpath):
  import os
  import glob
  import re

  nodeName = channel.gateway.id.split('-')[0] # remove the tailing '-slave'
  path = '%s/%s_catalog/DataCollector' % (catalogpath, nodeName)

  ddls = {}
  pathLen = len(path) + 1
  for f in glob.glob(path + "/CREATE_*.sql") :
    filename = f[pathLen:]
    if filename != "CREATE_ALL_TABLES.sql" :
      # skip 1st line
      with open(f) as fin :
        lines = fin.readlines()[1:]
      lines[0] = lines[0].replace(":dcschema.", "")
      # pattern: CREATE TABLE tablename(
      tablename = re.search("^(\s*\w+\s+\w+\s+)(\w+)", lines[0]).group(2)
      ddls[tablename] = "".join(lines)

  if not channel.isclosed():
    channel.send(ddls)
