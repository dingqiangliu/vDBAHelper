#!/usr/bin/python
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: SQLite virtual tables for Vertica data collectors
# Author: DingQiang Liu

import os, atexit
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
import re
from ConfigParser import ConfigParser

import execnet



def getVerticaCluster(vDbName = '', vMetaFile = '/opt/vertica/config/admintools.conf', vAdminOSUser = 'dbadmin'):
  """
  single instance of VerticaCluster
    Arguments:
      vDbName: string
        Vertica database name, if ignored, fist database in vMetaFile(/opt/vertica/config/admintools.conf) will be choosed.
      vMetaFile string
        Vertica database meta file
      vAdminOSUser string
        Vertica Administrator OS username 
  """
  global __g_verticaCluster
  
  try:
    if not __g_verticaCluster is None :
      return __g_verticaCluster
  except NameError:
    __g_verticaCluster = None

  if __g_verticaCluster is None :
    __g_verticaCluster = VerticaCluster(vDbName, vMetaFile, vAdminOSUser)

    # close VerticaCluster automatically when exiting
    atexit.register(destroyVerticaCluster)

  return __g_verticaCluster



def destroyVerticaCluster():
  """
  close VerticaCluster
  """
  global __g_verticaCluster

  try:
    if not __g_verticaCluster is None :
        __g_verticaCluster.destroy()
  except NameError:
    pass

  __g_verticaCluster = None



class VerticaCluster:
  def __init__(self, vDbName = '', vMetaFile = '/opt/vertica/config/admintools.conf', vAdminOSUser = 'dbadmin'):
    """ Register datacollectors of Vertica
    Arguments:
      vDbName: string
        Vertica database name, if ignored, fist database in vMetaFile(/opt/vertica/config/admintools.conf) will be choosed.
      vMetaFile string
        Vertica database meta file
      vAdminOSUser string
        Vertica Administrator OS username 
    """

    # get configurations(vDbName, path, node, host) of Vertica database
    self.vDbName, self.catPath, self.nodeNames, self.hostIPs = getVerticaDBConfig(vDbName, vMetaFile)
    self.vAdminOSUser = vAdminOSUser

    self.executors = execnet.Group()

    # create executors sequentially
    #self.initExecuters()

    # create executors in parallel
    self.initExecutersParallel()
    
  def initExecuters(self) :
    for i, host in enumerate(self.hostIPs):
      self.executors.makegateway("ssh=%s@%s//id=%s" % (self.vAdminOSUser, host, self.nodeNames[i]))
      
  def initExecutersParallel(self) :
    pool = ThreadPool()
    gws = pool.map(self.createExecuter, range(len(self.hostIPs)))
    pool.close()
    pool.join()
    for gw in gws :
      self.executors._register(gw)
    
  def createExecuter(self, i) :
    tg = execnet.Group()
    gw = tg.makegateway("ssh=%s@%s//id=%s" % (self.vAdminOSUser, self.hostIPs[i], self.nodeNames[i]))
    tg._unregister(gw)
    del gw._group
    return gw

  def destroy(self):
    if not self.executors is None :
      self.executors.terminate()
      self.executors = None


def getVerticaDBConfig(vDbName = '', vMetaFile = '/opt/vertica/config/admintools.conf'):
  """ get configurations of Vertica database
  Arguments:
    vDbName: string
      Vertica database name, if ignored, fist database in vMetaFile(/opt/vertica/config/admintools.conf) will be choosed.
    vMetaFile string
      Vertica database meta file
  Returns:
    vDbName: string
    catPath: string
      catalog pararent path
    nodeNames: list of nodename
    hostIPs: list of IP
  """

  # get configurations(path, node, host) of Vertica database
  try:
    configdict = ConfigParser()
    if not os.path.exists(vMetaFile):
      raise StandardError("Vertica database meta file [%s] not exists!" % vMetaFile)
    configdict.read(vMetaFile)
    
    pat = re.compile("^Database:%s" % vDbName)
    sections = [ d for d in configdict.sections() if pat.match(d) ]
    if len(sections) == 0 :
      raise StandardError("No Vertica database [%s] is defined in meta file [%s] not exists!" % (vDbName, vMetaFile))
    section = sections[0]
    if vDbName == '' :
      vDbName = section.split(":")[1]
    catPath = configdict.get(section, 'path')
    if len(catPath) == 0 :
      raise StandardError("No [path] property of Vertica database [%s] defined in meta file [%s]!" % (vDbName, vMetaFile))
    nodeNames = configdict.get(section, 'nodes').split(',')
    if len(nodeNames) == 0 or len(nodeNames[0]) == 0 :
      raise StandardError("No [nodes] property of Vertica database [%s] defined in meta file [%s]!" % (vDbName, vMetaFile))
    if configdict.has_option(section, 'host'):
        configdict.remove_option(section, 'host')
    hostIPs = [ configdict.get('Nodes', n).split(',')[0] for n in nodeNames]
    if len(hostIPs) != len(nodeNames) :
      raise StandardError("Nodes section of Vertica database [%s] defined in meta file [%s] is not correct!" % (vDbName, vMetaFile))
  
    return vDbName, catPath, nodeNames, hostIPs
  except Exception, e:
      raise StandardError("'%s' when getting configurations of Vertica database [%s] in meta file [%s]." % (str(e), vDbName, vMetaFile))
  
  return vDbName, None, None, None

