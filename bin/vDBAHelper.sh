#!/bin/bash
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: vDBAHelper
# Author: DingQiang Liu

ScriptDir=$(cd "$(dirname $0)"; pwd)
vDBAHome=$(cd "${ScriptDir}/.."; pwd)

if [ "$(uname)" == "Darwin" ] ; then
  export DYLD_LIBRARY_PATH="${vDBAHome}/lib":${DYLD_LIBRARY_PATH}
else
  export LD_LIBRARY_PATH="${vDBAHome}/lib":${LD_LIBRARY_PATH}
fi
SitePackagesDir="${vDBAHome}/eggs"
export PYTHONPATH="${SitePackagesDir}":${PYTHONPATH}
PYTHON="/opt/vertica/oss/python/bin/python"
[ ! -f "${PYTHON}" ] && PYTHON="$(which python)"

if [ "$(${PYTHON} -c 'import sys; print sys.version_info >= (2,7)')" != "True" ] ; then
  echo "python 2.7+ is required!"
  exit 1
fi
#apsw.so need it
if [ -f /usr/lib*/libpython*.so.1.0 ] ; then
  [ -f lib/libpython2.6.so.1.0 ] || ln -s /usr/lib*/libpython*.so.1.0 "${vDBAHome}/lib/libpython2.6.so.1.0"
  if [ ! $? ] ; then
    echo "libpython*.so.1.0 is not found!"
    exit 1
  fi
fi

etcDir="${vDBAHome}/etc"
mkdir -p "${etcDir}"
logsDir="${vDBAHome}/logs"
mkdir -p "${logsDir}"

"${PYTHON}" ${vDBAHome}/eggs/web/server.py "$@"
