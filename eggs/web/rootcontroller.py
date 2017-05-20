#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: default web controller
# Author: DingQiang Liu

import os
import logging

from bottle import route, error, static_file, template, request, response, HTTPResponse
import markdown2

logger = logging.getLogger(__name__)


@error(404)
def error404(error):
    """ logging 404 error """

    logger.error('%s %s %s %s' % (request.remote_addr,
                                            request.method,
                                            request.url,
                                            404))
    return '[%s] not exists, sorry:(' % request.url


@route('/<filepath:re:.*\.md>')
def root_markdown(filepath):
    return HTTPResponse(markdown2.markdown_path('%s/%s' % (os.path.dirname(os.path.realpath(__file__)), filepath)))


@route('/static/<filepath:path>')
def root_static(filepath):
    return static_file(filepath, root='%s/static' % os.path.dirname(os.path.realpath(__file__)))


@route('/')
def root():
    """ home page """
    return template('index')


@route('/<modulename>/')
def moduleroot(modulename):
    """ module home page """
    return template("%s/index" % modulename)


@route('/<name:path>')
def page(name):
    """ page """
    return template(name)
