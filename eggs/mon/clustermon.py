#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: cluster monitoring
# Author: DingQiang Liu

import os, sys
from optparse import OptionParser
import logging
import time
from cStringIO import StringIO
import re

import db.vcluster as vcluster
import db.dbmanager as dbmanager
import util.reflection as reflection


logger = logging.getLogger("clustermon")


def remotecall(src, args, nodeNamesPattern) :
    """ remotely execute script on Vertica cluster.

      Arguments:
        - src: string, python scriptlet.
        - args: dictionary of arguments for script.
        - nodeNamesPattern: regular expression pattern for select Vertica nodes.

      Returns: list of result from each nodes of Vertica cluster.
    """
    
    ret = {}
    vc = vcluster.getVerticaCluster()
    mch = vc.executors.remote_exec(src)
    mch.send_each(args)
    q = mch.make_receive_queue(endmarker=None)
    terminated = 0
    while 1:
      channel, result = q.get()
      if result is None :
        terminated += 1
        if terminated == len(mch):
          break
        continue
      else: 
        nodeName = channel.gateway.id
        ret.update({nodeName: result}) 

    return [ret[k] for k in [key for key in sorted(ret) if nodeNamesPattern.match(key) ]]


if __name__ == "__main__":
    old_stdout = sys.stdout
    sys.stdout = mystdout = StringIO() 
    import mon.dstat as dstatmodule
    import mon.clusterdstat as clusterdstatmodule
    sys.stdout = old_stdout
    srcclusterdstatmodule = reflection.overridemodule(dstatmodule, clusterdstatmodule)

    class MyOptionParser(OptionParser):
        def error(self, msg):
            pass

    options = None
    helpArgs = ['-h', '--help', '--list']
    if not [ a for a in sys.argv[1:] if a in helpArgs] :  
        parser = MyOptionParser()  
        parser.add_option("--database", dest="vDBName", default="") 
        parser.add_option("--file", dest="vMetaFile", default="/opt/vertica/config/admintools.conf") 
        parser.add_option("--user", dest="vAdminOSUser", default="dbadmin") 
        parser.add_option("--nodes", dest="nodeNamesExpress", default=".*") 
        (options, _) = parser.parse_args()

    args = ["--time", "--nodename", "--all", "--color", "--noupdate", "--noheaders"]
    skipnext = False
    for arg in sys.argv[1:] :
        if arg in ['--database', '--file', '--user', '--nodes']:
            skipnext = True
            continue
        if skipnext :
            skipnext = False
        else :
            args.append(arg)
    args = args + ["1", "0"] 

    if [ a for a in sys.argv[1:] if a in helpArgs] :  
        dstatmodule.Options(args)
        exit(0)

    dbmanager.setup(options.vDBName, options.vMetaFile, options.vAdminOSUser, "")
    nodeNamesPattern = re.compile(options.nodeNamesExpress)

    # init, get headers
    headers = ""
    src = srcclusterdstatmodule + """
if __name__ == '__channelexec__' or __name__ == '__main__' :
    nodeName = channel.gateway.id.split('-')[0] # remove the tailing '-slave'

    remoteargs = channel.receive()
    args = remoteargs["args"]
    dstatmodule.initterm()
    dstatmodule.op = dstatmodule.Options(args)
    dstatmodule.theme = dstatmodule.set_theme()
    dstatmodule.main()

    old_stdout = sys.stdout
    from cStringIO import StringIO
    sys.stdout = mystdout = StringIO() 
    dstatmodule.perform(0)
    sys.stdout = old_stdout
    channel.send(header(totlist, totlist))
    """
    for line in remotecall(src, {"args": args}, nodeNamesPattern) :
        #only get headers from 1st node
        headers = line
        break

    # get counters
    src = """
if __name__ == '__channelexec__' or __name__ == '__main__' :
    nodeName = channel.gateway.id.split('-')[0] # remove the tailing '-slave'

    remoteargs = channel.receive()
    update = remoteargs["update"]

    old_stdout = sys.stdout
    from cStringIO import StringIO
    sys.stdout = mystdout = StringIO() 
    dstatmodule.perform(update)
    sys.stdout = old_stdout
    channel.send(mystdout.getvalue())
    """
    try:
        update = 1000 # Seems there will be weird issue if update increase from 1 
        while True:
            print headers,
            for line in remotecall(src, {"update": update}, nodeNamesPattern) :
                print line,
            time.sleep(1)
            update += 1
            if update < 0 :
                update = 1000

    except KeyboardInterrupt, e:
        print

    sys.stdout.write('\n')
