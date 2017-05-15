#!/bin/bash
#
# Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
# Description: SQLite extensions for remotely accessing Vertica cluster info
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

vDbName=""
vMetaFile="/opt/vertica/config/admintools.conf"
vAdminOSUser="dbadmin"

# process and remove additional options
usage(){
  #"${PYTHON}" -c "import apsw;apsw.main()" "--help"
  "${PYTHON}" -c "import sys, apsw; apsw.Shell().process_args(sys.argv[1:])" "--help"
  cat <<-EOF
Vertica OPTIONS:
   -d | --database verticaDBName   Vertica database name, default is the first database in meta file(/opt/vertica/config/admintools.conf)
   -f | --file verticaMetaFile     Vertica database meta file, default is (/opt/vertica/config/admintools.conf)
   -u | --user verticaAdminOSUser  Vertica Administrator OS username, default is dbadmin
   ------------------------
   -h | --help                     show usage
Notes: you should confirm ssh password-less accessible those nodes IPs in Vertica database meta file with verticaAdminOSUser

	EOF
  exit 1
}
args=("$@")
argsCount=${#args[*]}
for (( n=0 ; n<argsCount ; n++ )) ; do
  case "${args[$n]}" in
	-d | --database ) unset args[$n]; n=$((n+1)); vDbName="${args[$n]}"; unset args[$n] ;;
	-f | --file ) unset args[$n]; n=$((n+1)); vMetaFile="${args[$n]}"; unset args[$n] ;;
	-u | --user ) unset args[$n]; n=$((n+1)); vAdminOSUser="${args[$n]}"; unset args[$n] ;;
    -h | --help )    unset args[$n]; usage ;;
  esac
done
set -- "${args[@]}"

# igore Ctrl-C, avoid Ctrl-C kill ssh remote communication process when stoping current input in shell
if [ "${vMetaFile}" != "" ] ; then
  trap '' SIGINT
fi

pscript=$(cat <<-EOF
	import os
	from logging.config import fileConfig
	# set up the logger
	fileConfig(os.path.realpath("${vDBAHome}/etc/logging.ini"))

	import apsw
	import sys
	import signal
	import time
	
	import db.dbmanager as dbmanager
	import db.vsource as vsource

	s=apsw.Shell()
	argsOptionAndDbfile=[a for a in sys.argv[1:] if a.startswith("-")]
	argsCmdSQL=[a for a in sys.argv[1:] if not a.startswith("-")]
	if len(argsCmdSQL) > 0 :
	  argsOptionAndDbfile.append(argsCmdSQL.pop(0))
	
	s.process_args(argsOptionAndDbfile)
	
	dbmanager.setup('${vDbName}', '${vMetaFile}', '${vAdminOSUser}', connection = s.db)

	if len(argsCmdSQL) > 0 :
	  for cs in argsCmdSQL :
	    if cs.startswith(".") :
	      s.process_command(cs)
	    else:
	      s.process_sql(cs)
	else:
	  # tell background sync job it's busy now.
	  __process_sql = s.process_sql
	  def wrapProcessSQL(*args, **kargs) :
	    vsource.setLastSQLiteActivityTime(time.time())
	    __process_sql(*args, **kargs)
	    vsource.setLastSQLiteActivityTime(time.time())
	  s.process_sql = wrapProcessSQL
    
	  __process_complete_line = s.process_complete_line
	  def wrapProcessCmdLine(*args, **kargs) :
	    s.db.interrupt()
	    vsource.setLastSQLiteActivityTime(time.time())
	    __process_complete_line(*args, **kargs)
	  s.process_complete_line = wrapProcessCmdLine

	  intro="""
Welcome to vDBAHelper! 
Powered by SQLite version %s (APSW %s)
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
""" % (apsw.sqlitelibversion(), apsw.apswversion())
	  s.cmdloop(intro)
	
EOF
)
"${PYTHON}" -c "${pscript}" "$@"

