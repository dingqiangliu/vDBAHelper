#!/usr/bin/python
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: SQLite virtual tables for Vertica data collectors
# Author: DingQiang Liu

from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from datetime import datetime
from decimal import Decimal
import struct

import db.vcluster as vcluster

def setup(connection):
  """ Register datacollectors of Vertica
  Arguments:
    connection: apsw.Connection
  """

  DatacollectorSource(connection);


# module for datacollector
class DatacollectorSource:
  def __init__(self, connection):
    self.tables = {}
    self.connection = connection
    self.connection.createmodule("verticadc", self)
  
    # create datacollector virtual tables
    vc = vcluster.getVerticaCluster()
    self.ddls = vc.executors[0].remote_exec(getDDLs, catalogpath=vc.catPath).receive()

    cursor = None 
    try :
      cursor = self.connection.cursor()
      for tableName in self.ddls :
        cursor.execute("create virtual table if not exists %s using verticadc" % tableName)

      for tableName in self.ddls :
        self.tables[tableName].columns = [ columnName.upper() for _, columnName, _, _, _, _ in cursor.execute("pragma table_info('%s');" % tableName) ]
        self.tables[tableName].columns.insert(0, "ROWID")
        # only keep the first part of SQL typename
        self.tables[tableName].columnTypes = { columnName.upper(): columnType.split(' ')[0].split('(')[0].upper() for _, columnName, columnType, _, _, _ in cursor.execute("pragma table_info('%s');" % tableName) }
        self.tables[tableName].columnTypes["ROWID"] = "INTEGER"

      # TODO: start data sync job if SQLite is not empty or memory
      if self.connection.filename != '' :
        print "TODO: filename =", self.connection.filename
    finally :
      if not cursor is None :
        cursor.close();


  def Create(self, db, modulename, dbname, tablename, *args):
    table = Table(tablename)
    self.tables[tablename] = table
    return self.ddls[tablename], table

  Connect=Create
  

# table for datacollector
class Table:
  def __init__(self, tablename):
    self.tablename=tablename
    self.columns = []
    self.columnTypes = {}

  def BestIndex(self, *args):
    # TODO: index on nodename and time
    return None

  def Open(self):
    return Cursor(self)

  def Disconnect(self):
    pass

  Destroy=Disconnect

    

# cursor for datacollector
class Cursor:
  def __init__(self, table):
    self.table = table
    self.data = None


  def Filter(self, *args):
    self.pos=0

    if self.data is None:
      columns = self.table.columns

      # get data 
      self.data = []
      vc = vcluster.getVerticaCluster()
      mch = vc.executors.remote_exec(getTableData, catalogpath=vc.catPath, tablename=self.table.tablename, columns=columns)
      q = mch.make_receive_queue(endmarker=None)
      terminated = 0
      while 1:
        channel, rows = q.get()
        if rows is None :
          terminated += 1
          if terminated == len(mch):
            break
          continue
        else: 
          # TODO: multiple thread parsing for better performance
          self.data.extend(self.parseRows(rows, columns))
          #self.data.extend(self.parseRowsParallel(rows, columns))

  def parseRowsParallel(self, rows, columns):
    # multiple thread parsing for better performance
    pool = ThreadPool()
    dataRows = pool.map( partial(self.parseLine, columns=columns) , [ line for line in rows.split('\2') ] )
    pool.close()
    pool.join()
    return [x for x in dataRows if x is not None] # ignore broken line 

  def parseRows(self, rows, columns):
    dataRows = []
    for line in rows.split('\2'):
      try :
        # ignore broken line
        colValues = line.split('\1')
        if len(colValues) == len(columns) :
          dataRows.append( [ parseValue(self.table.columnTypes[columns[i]], cv) for i, cv in enumerate(colValues) ] )
      except Exception, e:
        raise StandardError("[%s] when parseRows [%s] of column [%s] on table [%s]" % (str(e), cv, self.table.columns[i], self.table.tablename))
    return dataRows

  def parseLine(self, line, columns):
    try :
      colValues = line.split('\1')
      if len(colValues) == len(columns) :
        return [ parseValue(self.table.columnTypes[columns[i]], cv) for i, cv in enumerate(colValues) ]
      else :
        return None
    except Exception, e:
      raise StandardError("[%s] when parseLine [%s] of column [%s] on table [%s]" % (str(e), cv, self.table.columns[i], self.table.tablename))

  def Eof(self):
    return self.pos>=len(self.data)

  def Rowid(self):
    return self.data[self.pos][0]

  def Column(self, col):
    try :
      return self.data[self.pos][1+col]
    except Exception, e:
      raise StandardError("[%s] when get value [%s] of column [%s] on table [%s]" % (str(e), self.data[self.pos][1+col], self.table.columns[1+col], self.table.tablename))

  def Next(self):
    self.pos+=1

  def Close(self):
    self.data = None


def parseValue(sqltype, value):
  # Till now, Vertica datacollector tables only use types: BOOLEAN, FLOAT, INTEGER, TIMESTAMP WITH TIME ZONE, VARCHAR
  if len(value) == 0:
    return None
  
  try :
    if sqltype in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'MEDIUMINT', 'TINYINT', 'INT2', 'INT8') :
      # convert unsigned long to negative long. Note: INTEGER is numeric(18,0) in Vertica, eg. '18442240474082184385' means -4503599627367231
      lValue = long(value)
      if lValue <= 0x7fffffffffffffff :
        return lValue
      else :
        return struct.unpack('l', struct.pack('L', lValue))[0] 
    elif sqltype in ('DOUBLE', 'FLOAT', 'REAL') :
      return float(value)
    elif sqltype in ('DATE', 'DATETIME', 'TIMESTAMP') :
      lValue = long(value)
      # -9223372036854775808(-0x8000000000000000) means null in Vertica
      if lValue == -0x8000000000000000 :
          return None
      # 946684800 is secondes between '1970-01-01 00:00:00'(Python) and '2000-01-01 00:00:00'(Vertica)
      return datetime.fromtimestamp(float(lValue)/1000000+946684800).strftime("%Y-%m-%d %H:%M:%S.%f")
    elif sqltype == 'BOOLEAN' :
        return 'true' == value.lower()
    elif sqltype in ('DECIMAL', 'NUMERIC', 'BOOLEAN') :
      return Decimal(value)
    elif sqltype == 'BLOB' :
      return buffer(value)
    else :
      # others are str. 
      # process escpe character in string, eg. '\n'
      return value.decode('string_escape')
  except :
    # ignore incorrect value format
    return None


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


def getTableData(channel, catalogpath, tablename, columns):
  from multiprocessing.dummy import Pool as ThreadPool
  from functools import partial
  import os
  import glob
  
  data=[]

  batchsize=100000

  nodeName = channel.gateway.id.split('-')[0] # remove the tailing '-slave'
  path = '%s/%s_catalog/DataCollector' % (catalogpath, nodeName)
  nodeNum = int(nodeName[-4:])

  # log filename rule from tablename: remove leading 'dc_', remove '_' and capitalize first character of each word
  logFilePatten = "".join([w.capitalize() for w in tablename.split('_')[1:] ])
  recBegin=":DC" + logFilePatten
  recEnd="." 
  counter=1
  dictColumns = { columnName: None for columnName in columns } # Note: lookup in dictionary except array for better performace 
  # TODO: multiple thread parsing for better performance, but keep time order?
  for f in glob.glob(path + "/" + logFilePatten + "_*.log"):
    try :
      with open(f) as fin :
        row = []
        for line in fin :
          line = line[:-1] # remove tailing '\n'
          if line == recBegin :
            pass
            # ROWID
            row.append(str(counter*10000 + nodeNum))
          elif line == recEnd :
            if len(row) > 0 :
              data.append('\1'.join(row))

              row = []
              counter+=1
            if counter % batchsize == 0 :
              channel.send('\2'.join(data))
              data = []
          else :
            lparts = line.split(":")
            columnName = lparts[0].upper()
            if columnName in dictColumns :
              columnValue = ":".join(lparts[1:]) if len(lparts) > 0 else ""
              row.append(columnValue)
    except IOError, e :
      # ignore "IOError: [Errno 2] No such file or directory...", when datacollectors file rotating
      if 'No such file or directory' in str(e) :
        pass
    
  if not channel.isclosed() and len(data) > 0 :
    channel.send('\2'.join(data))
    data = []


