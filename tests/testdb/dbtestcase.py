#!/usr/bin/python
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: testing cases for SQLite virtual tables of Vertica data collectors
# Author: DingQiang Liu

import unittest

import apsw

import db.vdatacollectors as vdatacollectors


def getConnection() :
  """
  Get single instance sharing db connection.
  """

  global __g_Connection
  try:
    if not __g_Connection is None :
      return __g_Connection
  except NameError:
    __g_Connection = None
  return __g_Connection

  
def setConnection(connection) :
  """
  Set single instance sharing db connection.
  """

  global __g_Connection
  __g_Connection = connection



class DBTestCase(unittest.TestCase):
  """
  Base class for db related test cases.
  """

  def setUp(self):
    self.newConnection = False
    self.connection = getConnection()

    if self.connection is None :
      self.newConnection = True
      self.connection = apsw.Connection(":memory:")
      vdatacollectors.setup(self.connection)
    
  def tearDown(self):
    if self.newConnection : 
      self.connection.close()
    self.connection = None
      