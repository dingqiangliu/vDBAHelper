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


etcDir="$(cd "${ScriptDir}/../etc"; pwd)"
mkdir -p "${etcDir}"

"${ScriptDir}/elinks.$(uname)" -config-dir "${etcDir}" "$@"
