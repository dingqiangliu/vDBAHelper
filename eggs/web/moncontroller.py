#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: Monitoring web controller
# Author: DingQiang Liu

import logging
import traceback
import re

from bottle import route, post, error, static_file, template, request, response, HTTPError
from ansi2html import Ansi2HTMLConverter 

from mon import clustermon

logger = logging.getLogger(__name__)

headers = None
update = 1000 # Seems there will be weird issue if update increase from 1 

@route('/mon/monlist')
@post('/mon/monlist')
def monlist(db):
    try: 
        nodenamepattern = request.forms.get('nodenamepattern')
        if nodenamepattern is None or len(nodenamepattern) == 0:
            nodenamepattern = ".*"

        return template("mon/monlist", nodenamepattern=nodenamepattern)
    except Exception, e:
        msg = "Failed when get mon list!"
        return HTTPError(body=msg, exception=e, traceback=traceback.format_exc())


@route('/mon/mondetail')
def mondetail(db):
    try: 
        global headers, update

        start = request.params['start']
        if start is None or len(start) == 0 :
            start = "1"

        nodeNamesPattern = re.compile(request.params['nodenamepattern'])
        if headers is None :
            args = ["--time", "--nodename", "--all", "--color", "--noupdate", "--noheaders", "--bw", "1", "0"]
            headers = clustermon.initmonitor(args, nodeNamesPattern)
        ansioutput = headers + ''.join(clustermon.monitoring(update, nodeNamesPattern))
        update += 1
        if update < 0 :
            update = 1000

        conv = Ansi2HTMLConverter(dark_bg=True, scheme='xterm', title='Monitoring')
        htmloutput = conv.convert(ansioutput, full=True, ensure_trailing_newline=True)
        if start == "1" :
            htmloutput = htmloutput.replace('<head>', '<head>\n<meta http-equiv="refresh" content="1;" />')

        return htmloutput
    except Exception, e:
        msg = "Failed when get mon detail!"
        return HTTPError(body=msg, exception=e, traceback=traceback.format_exc())
