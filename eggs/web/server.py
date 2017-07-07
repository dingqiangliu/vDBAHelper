#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: webserver
# Author: DingQiang Liu

import os
from logging.config import fileConfig
# set up the logger
vDBAHome = os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + "/../..")
fileConfig("%s/etc/logging.ini" % vDBAHome, defaults={"logdir": "%s/logs" % vDBAHome})

import sys
import traceback
import subprocess
from functools import partial, wraps
import threading
from optparse import OptionParser
import re
import time
import logging
import traceback, signal

import bottle

import db.dbmanager as dbmanager
from web.DBPlugin import setConnection, getConnection, Plugin


logger = logging.getLogger("web.server")


def discoverwebcontroller(start_dir=None, pattern='.*controller\.py$', top_level_dir=None) :
    """Find and import all web modules.
    """

    if start_dir is None :
        import web
        start_dir = os.path.dirname(os.path.realpath(web.__file__))
        top_level_dir = start_dir[:-1-len(web.__name__)]

    pat = pattern
    if type(pat) is str :
        pat = re.compile(pat)

    paths = os.listdir(start_dir)
    for path in paths:
        full_path = os.path.join(start_dir, path)
        if os.path.isfile(full_path):
            if pat.match(full_path):
                modulename = full_path[len(top_level_dir)+1:-3].replace(os.path.sep, '.')
                __import__(modulename)
                viewDir = start_dir + '/views'
                if os.path.exists(viewDir) and not viewDir in bottle.TEMPLATE_PATH :
                    bottle.TEMPLATE_PATH.insert(0, viewDir)
        else:
            discoverwebcontroller(full_path, pat, top_level_dir)


def prepareWebserver(host="localhost", port=8080):
    """
    start web server.
    """

    def log_to_logger(fn):
        '''
        Wrap a Bottle request so that a log line is emitted after it's handled.
        (This decorator can be extended to take the desired logger as a param.)
        '''
        @wraps(fn)
        def _log_to_logger(*args, **kwargs):
            request_time = time.time()
            res = None
            try:
                res = fn(*args, **kwargs)
                if isinstance(res, bottle.HTTPError):
                    logger.error('%s %s %s %s  Caused by: %s. Exception:%s\n%s' % (bottle.request.remote_addr,
                                                bottle.request.method,
                                                bottle.request.url,
                                                res.status,
                                                res.body, 
                                                str(res.exception),
                                                res.traceback))
                    res.exception = None
                    res.traceback = None
                else:
                    logger.info('%s %s %s %s finished in %.1f secondes' % (bottle.request.remote_addr,
                                                bottle.request.method,
                                                bottle.request.url,
                                                bottle.response.status,
                                                time.time() - request_time))
            except:
                logger.exception('%s %s %s %s' % (bottle.request.remote_addr,
                                            bottle.request.method,
                                            bottle.request.url,
                                            500))

            return res 
        return _log_to_logger

    #import all web modules
    bottle.install(Plugin())
    discoverwebcontroller()
    bottle.install(log_to_logger)


def startBrowser(host="localhost", port=8080):
    """
    start web browser.
    """

    home = os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + "/../..")
    args = ["%s/bin/elinks.sh" % home, "http://%s:%s/" % (host, port)]
    return subprocess.call(args)

class LoggerWriter:
    def __init__(self, level):
        # self.level is really like using logger.info/error(message)
        self.level = level

    def write(self, message):
        # if statement reduces the amount of newlines that are printed to the logger
        if message != '\n':
            self.level(message)

    def flush(self):
        # create a flush method so things can be flushed when
        # the system wants to. Not sure if simply 'printing'
        # sys.stderr is the correct way to do it, but it seemed
        # to work properly for me.
        self.level(sys.stderr)

def dumpstacks(signal, frame):
    id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
    code = []
    code.append("\n############ Threads stack ############") 
    for threadId, stack in sys._current_frames().items():
        code.append("# Thread: %s(%d)" % (id2name.get(threadId,""), threadId))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('%sFile: "%s", line %d, in %s' % (' ' * 2, filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
    print "\n".join(code)

if __name__ == "__main__":
    # catch 'kill -SIGQUIT' or 'kill -3' for dumping threads stack
    signal.signal(signal.SIGQUIT, dumpstacks)
    
    parser = OptionParser()
    parser.add_option("-d", "--database", dest="vDBName", default="", help="Vertica database name, default is the first database in meta file(/opt/vertica/config/admintools.conf)") 
    parser.add_option("-f", "--file", dest="vMetaFile", default="/opt/vertica/config/admintools.conf", help="Vertica database meta file, default is /opt/vertica/config/admintools.conf") 
    parser.add_option("-u", "--user", dest="vAdminOSUser", default="dbadmin", help="Vertica Administrator OS username, default is dbadmin") 
    (options, args) = parser.parse_args()
    
    sqliteDBFile = ""
    if len(args) > 0 :
        sqliteDBFile = args[0]
    
    connection = dbmanager.setup(options.vDBName, options.vMetaFile, options.vAdminOSUser, sqliteDBFile)
    setConnection(connection)


    # start web server
    successful = False
    try:
        host, port = "localhost", 8080
        prepareWebserver(host=host, port=port)
        t = threading.Thread(target=partial(bottle.run, host=host, port=port, quiet=True, debug=True))
        t.daemon = True
        t.start()
        successful = True
        logger.info("web server started on %s:%s" % (host, port))
    except Exception as e:
        print "Failed to start web server on %s:%s ! Reason: %s\n%s" % (host, port, str(e), traceback.format_exc())
        logger.exception("Failed to start web server on %s:%s ! Reason: %s" % (host, port, str(e)))

    # start browser
    if successful:
        logger.info("starting web browser...")
        # redirect stdout/stderror to logger to avoid ruining screen
        sys.stdout = LoggerWriter(logger.info)
        sys.stderr = LoggerWriter(logger.error)
        startBrowser(host, port)
        logger.info("web browser exited.")
