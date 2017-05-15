#!/usr/bin/python
#encoding: utf-8
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: APSW plugin, copy from Bottle-sqlite
# Author: DingQiang Liu


'''
DBPlugin is a plugin that integrates APSW/SQLite with your Bottle
application. It automatically connects to a database at the beginning of a
request, passes the database handle to the route callback and closes the
connection afterwards.

To automatically detect routes that need a database cusor, the plugin
searches for route callbacks that require a `db` keyword argument
(configurable) and skips routes that do not. This removes any overhead for
routes that don't need a database connection.

Usage Example::

    import bottle
    import web.DBPlugin

    app = bottle.Bottle()
    plugin = DBPlugin.Plugin()
    app.install(plugin)

    @app.route('/show/:item')
    def show(item, db):
        row = db.execute('SELECT * from items where name=?', item)
        if row:
            return template('showitem', page=row)
        return HTTPError(404, "Page not found")
'''

import time
import inspect

from bottle import PluginError

import db.vsource as vsource


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


class APSWPlugin(object):
    ''' This plugin passes an APSW connection to route callbacks
    that accept a `db` keyword argument. If a callback does not expect
    such a parameter, no connection is made. You can override the database
    settings on a per-route basis. '''

    name = 'APSW'
    api  = 2

    def __init__(self, keyword='db'):
         self.keyword = keyword

    def setup(self, app):
        ''' Make sure that other installed plugins don't affect the same
            keyword argument.'''
        for other in app.plugins:
            if not isinstance(other, APSWPlugin): continue
            if other.keyword == self.keyword:
                raise PluginError("Found another APSWPlugin plugin with "\
                "conflicting settings (non-unique keyword).")

    def apply(self, callback, route):
        keyword = self.keyword

        # Test if the original callback accepts a 'db' keyword.
        # Ignore it if it does not need a database handle.
        args = inspect.getargspec(route.callback)[0]
        if keyword not in args:
            return callback

        def wrapper(*args, **kwargs):
            # Connect to the database
            db = None
            con = getConnection()
            if not con is None:
                db = con.cursor()
            # Add the connection handle as a keyword argument.
            kwargs[keyword] = db

            con.interrupt()
            vsource.setLastSQLiteActivityTime(time.time())
            res = callback(*args, **kwargs)
            kwargs[keyword] = None
            db.close()
            db = None
            vsource.setLastSQLiteActivityTime(time.time())
            return res

        # Replace the route callback with the wrapped one.
        return wrapper

Plugin = APSWPlugin
