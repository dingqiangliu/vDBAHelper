#!/bin/bash
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: testing cases util for vDBAHelper 
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

profile=0

# process and remove additional options
usage(){
  cat <<-EOF
Usage: ${0} [OPTIONS] [SQLiteDbFile]
Vertica OPTIONS:
   -d | --database VerticaDBName   Vertica database name, default is the first database in meta file(/opt/vertica/config/admintools.conf)
   -f | --file verticaMetaFile     Vertica database meta file, default is (/opt/vertica/config/admintools.conf)
   -u | --user verticaAdminOSUser  Vertica Administrator OS username, default is dbadmin
   ------------------------
   -p | --profile                  profiling tests
   -h | --help                     show usage
Notes: you should confirm ssh password-less accessible those nodes IPs in Vertica database meta file with verticaAdminOSUser

	EOF
  exit 1
}
args=("$@")
argsCount=${#args[*]}
for (( n=0 ; n<argsCount ; n++ )) ; do
  case "${args[$n]}" in
	-p | --profile ) unset args[$n]; profile=1 ;;
    -h | --help )    unset args[$n]; usage ;;
  esac
done
set -- "${args[@]}"


if ((profile)) ; then
  [ -f /tmp/profile.out ] && rm -f /tmp/profile.out
  "${PYTHON}" -m cProfile -o /tmp/profile.out ${ScriptDir}/alltests.py "$@"
  "${PYTHON}" -c "import pstats; p=pstats.Stats('/tmp/profile.out'); p.sort_stats('cumtime').print_stats()"
else
  "${PYTHON}" ${ScriptDir}/alltests.py "$@"
fi

