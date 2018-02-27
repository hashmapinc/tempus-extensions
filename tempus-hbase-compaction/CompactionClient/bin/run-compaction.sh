#! /usr/bin/env bash
#
# run.sh:  run the CompactionClient by setting up the Java CLASSPATH.
#

# get the current directory
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# check command line args
if [[ $# != 5 ]]; then
  echo "usage: $0 <Compaction Jar Filename> <Full Class Name of CompactionClient> <Properties file name> <log4j Properties file> <deploy-mode [local|yarn]>"
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
#if ["$HADOOP_CLASSPATH" == ""]; then
#	HADOOP_CLASSPATH=`hadoop classpath`
#fi

CLASSPATH=${HBASE_CONF_DIR}:$1

EXTRA_DRIVER_OPTIONS="-Dpid=$$ -Dlog4j.configuration=file:$4"
#Local Testing
if [ "$5" == "local" ]; then
spark-submit --master local[16] \
            --driver-class-path $CLASSPATH \
            --driver-java-options "${EXTRA_DRIVER_OPTIONS}" \
            --class $2 \
            $1 --props $3
fi

#Live CLuster
if [ "$5" == "yarn" ]; then
spark-submit --master yarn \
            --deploy-mode client \
            --files /usr/hdp/current/hbase-client/conf/hbase-site.xml \
            --conf spark.yarn.maxAppAttempts=1 \
            --num-executors 4 \
            --executor-memory 2g \
            --driver-memory 2g \
            --executor-cores 3 \
            --queue default \
            --driver-class-path $CLASSPATH \
            --driver-java-options "${EXTRA_DRIVER_OPTIONS}" \
            --class $2 \
            $1 --props $3 \
fi
echo "Options : ${OPTIONS}"
#--master local[4] --driver-class-path $CLASSPATH --driver-java-options "-Dpid=$$ -Dlog4j
# .configuration=file:$4" --class $2 $1 --props $3

#--conf "spark.executor.extraClassPath=/usr/hdp/current/hbase-client/lib/hbase-client.jar:/usr/hdp/current/hbase-client/lib/hbase-common.jar:/usr/hdp/current/hbase-client/lib/hbase-server.jar:/usr/hdp/current/hbase-client/lib/guava-11.0.1.jar:/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar:twill-zookeeper-0.9.0.jar:phoenix-spark-4.7.0.2.5.3.0-37.jar:phoenix-core-4.7.0.2.5.3.0-37.jar:tephra-core-0.7.0.jar:tephra-api-0.7.0.jar:twill-discovery-api-0.9.0.jar:twill-common-0.9.0.jar:twill-discovery-api-0.6.0-incubating.jar:cdap-cli-3.3.0.jar:twill-discovery-core-0.9.0.jar:terajdbc4.jar:tdgssconfig.jar:spark-csv_2.10-1.3.0.jar:commons-csv-1.4.jar" \
#--conf "spark.driver.extraClassPath=/usr/hdp/current/hbase-client/lib/hbase-client.jar:/usr/hdp/current/hbase-client/lib/hbase-common.jar:/usr/hdp/current/hbase-client/lib/hbase-server.jar:/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar:/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar:phoenix-spark-4.7.0.2.5.3.0-37.jar:phoenix-core-4.7.0.2.5.3.0-37.jar:tephra-core-0.7.0.jar:twill-zookeeper-0.9.0.jar:tephra-api-0.7.0.jar:twill-discovery-api-0.9.0.jar:twill-common-0.9.0.jar:twill-discovery-api-0.6.0-incubating.jar:cdap-cli-3.3.0.jar:twill-discovery-core-0.9.0.jar:terajdbc4.jar:tdgssconfig.jar:spark-csv_2.10-1.3.0.jar:commons-csv-1.4.jar" \
#--jars "datanucleus-api-jdo-3.2.6.jar,phoenix-core-4.7.0.2.5.3.0-37.jar,datanucleus-core-3.2.10.jar,phoenix-spark-4.7.0.2.5.3.0-37.jar,datanucleus-rdbms-3.2.9.jar,guava-12.0.1.jar,metrics-core-2.2.0.jar,twill-zookeeper-0.9.0.jar,tephra-core-0.7.0.jar,twill-discovery-api-0.9.0.jar,tephra-api-0.7.0.jar,twill-common-0.9.0.jar,twill-discovery-api-0.6.0-incubating.jar,cdap-cli-3.3.0.jar,twill-discovery-core-0.9.0.jar,terajdbc4.jar,tdgssconfig.jar,spark-csv_2.10-1.3.0.jar,commons-csv-1.4.jar"