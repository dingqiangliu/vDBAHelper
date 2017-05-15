#!/bin/bash
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: vDBAHelper
# Author: DingQiang Liu

ScriptDir=$(cd "$(dirname $0)"; pwd)

if [ "$(uname)" == "Darwin" ] ; then
  export DYLD_LIBRARY_PATH="${ScriptDir}/../lib":${DYLD_LIBRARY_PATH}
else
  export LD_LIBRARY_PATH="${ScriptDir}/../lib":${LD_LIBRARY_PATH}
fi
SitePackagesDir="${ScriptDir}/../eggs"
export PYTHONPATH="${SitePackagesDir}":${PYTHONPATH}
PYTHON="/opt/vertica/oss/python/bin/python"
[ ! -f "${PYTHON}" ] && PYTHON="$(which python)"

if [ "$(${PYTHON} -c 'import sys; print sys.version_info >= (2,7)')" != "True" ] ; then
  echo "python 2.7+ is required!"
  exit 1
fi
#apsw.so need it
if [ -f /usr/lib*/libpython*.so.1.0 ] ; then
  [ -f lib/libpython2.6.so.1.0 ] || ln -s /usr/lib*/libpython*.so.1.0 "${ScriptDir}/../lib/libpython2.6.so.1.0"
  if [ ! $? ] ; then
    echo "libpython*.so.1.0 is not found!"
    exit 1
  fi
fi

etcDir="$(cd "${ScriptDir}/../etc"; pwd)"
mkdir -p "${etcDir}"
logsDir="$(cd "${ScriptDir}/../logs"; pwd)"
mkdir -p "${logsDir}"

"${PYTHON}" ${ScriptDir}/../eggs/web/server.py "$@"
