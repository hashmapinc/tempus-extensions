#! /usr/bin/env bash
#
# run.sh:  run the CompactionClient by setting up the Java CLASSPATH.
#

# get the current directory
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# check command line args
if [[ $# != 4 ]]; then
  echo "usage: $0 <Compaction Jar Filename> <Full Class Name of CompactionClient> <Properties file name> <log4j Properties file>"
  exit 1;
fi

if [ ! -f "$1" ]; then
	echo "Jar file $1 not present. Please check the location and try again."
	exit 1;
fi

if [ ! -f "$3" ]; then
	echo "Properties file $3 not present. Please check the location and try again."
	exit 1;
fi

if [ ! -f "$4" ]; then
        echo "log4j Properties file $4 not present. Please check the location and try again."
        exit 1;
fi

DEFAULT_HBASE_CONF_DIR="/etc/hbase/conf"
if [ "$HBASE_CONF_DIR" == "" ]; then
	echo "HBASE_CONF_DIR environment is absent. Setting it up with $DEFAULT_HBASE_CONF_DIR"
	echo "If you want to pick HBase conf file from anyother location, please set env for HBASE_CONF_DIR"
  	HBASE_CONF_DIR=$DEFAULT_HBASE_CONF_DIR
fi

# classpath initially contains $HBASE_CONF_DIR and `hadoop classpath`
#if ["$HADOOP_CLASSPATH" == ""]; then#
#	HADOOP_CLASSPATH=`hadoop classpath`
#fi

CLASSPATH=${HBASE_CONF_DIR}:$1

EXTRA_DRIVER_OPTIONS="-Dpid=$$ -Dlog4j.configuration=file:$4"


JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx2048m

"$JAVA" ${JAVA_HEAP_MAX} ${EXTRA_DRIVER_OPTIONS} -classpath "${CLASSPATH}" $2 --props $3