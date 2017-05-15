#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: APSW/SQLite management module.
# Author: DingQiang Liu

import os
import glob
import re
import logging

import apsw

import db.vcluster as vcluster
import db.vsource as vsource


logger = logging.getLogger(__name__)


def setup(vDbName = '', vMetaFile = '/opt/vertica/config/admintools.conf', vAdminOSUser = 'dbadmin', sqliteDBFile = '', connection = None):
    """ Initialize APSW/SQLite
    Arguments:
        vDbName: string
          Vertica database name, if ignored, fist database in vMetaFile(/opt/vertica/config/admintools.conf) will be choosed.
        vMetaFile string
          Vertica database meta file
        vAdminOSUser string
          Vertica Administrator OS username 
        sqliteDBFile string
          local SQLite db file 
        connection: apsw.Connection, default is None
    Return: apsw.Connection. Using connection argument if it's not None, otherwise a new connection will be created. 
    """

    vc = None
    try :
        if len(vMetaFile) > 0:
            vc = vcluster.getVerticaCluster(vDbName = vDbName, vMetaFile = vMetaFile, vAdminOSUser = vAdminOSUser)
        else :
            msg = "will not connect to Vertica cluster. You can not access newest info of Vertica." 
            print "Notice: %s" % msg
            logger.info(msg)
    except Exception:
        msg = "connect to Vertica cluster failed. You can not access newest info of Vertica." 
        print "ERROR: %s" % msg
        logger.exception(msg)

    if connection is None :
        connection = apsw.Connection(sqliteDBFile)

    if not vc is None :
        vsource.setup(connection)

    # install etc/*.sql
    cursor = None
    try :
        cursor = connection.cursor()
        for f in glob.glob(os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + "/../../etc/*.sql")):
            tablename = os.path.splitext(os.path.basename(f))[0]
            if len(cursor.execute("select tbl_name from sqlite_master where lower(tbl_name)  = ?", (tablename, )).fetchall()) == 0 :
                # execute earch sql in file
                logger.info("begin to install [%s] ..." % tablename)
                for sql in getSQLFromFile(f) :
                    cursor.execute(sql)
                logger.info("finish intall [%s] ." % tablename)
            else:
                logger.info("ignore install [%s] as it has been existed." % tablename)
    except Exception:
        logger.exception("Failed when install etc/*.sql")
    finally:
        if not cursor is None:
            cursor.close()
            cursor = None

    return connection


def getSQLFromFile(f) :
    text = ""
    with open(f) as fin :
        text = '\n'.join(fin.readlines())

    # remove block comments 
    text =  re.sub(re.compile("/\*.*?\*/", re.DOTALL), "", text)
    # remove line comments 
    text =  re.sub(re.compile("\-\-.*\n"), "", text)

    for sql in text.split(";"):
        yield sql
