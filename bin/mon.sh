#!/bin/bash
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: cluster monitoring
# Author: DingQiang Liu

ScriptDir=$(cd "$(dirname $0)"; pwd)
export LD_LIBRARY_PATH="${ScriptDir}/../lib":${LD_LIBRARY_PATH}
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

# process and remove additional options
usage(){
  cat <<-EOF
Usage: ${0} [OPTIONS] 
Vertica OPTIONS:
   --database VerticaDBName   Vertica database name, default is the first database in meta file(/opt/vertica/config/admintools.conf)
   --file verticaMetaFile     Vertica database meta file, default is (/opt/vertica/config/admintools.conf)
   --user verticaAdminOSUser  Vertica Administrator OS username, default is dbadmin
   --nodes verticaNodeNamePattern Regular expression for select Vertica nodes,  default is .* for all nodes
   -h | --help                show usage
Notes: you should confirm ssh password-less accessible those nodes IPs in Vertica database meta file with verticaAdminOSUser
------------------------
	EOF

  "${PYTHON}" ${SitePackagesDir}/mon/clustermon.py --help 
  
  exit 1
}

args=("$@")
argsCount=${#args[*]}
for (( n=0 ; n<argsCount ; n++ )) ; do
  case "${args[$n]}" in
    -h | --help )    unset args[$n]; usage ;;
  esac
done
set -- "${args[@]}"


"${PYTHON}" ${SitePackagesDir}/mon/clustermon.py "$@"

