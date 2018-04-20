# Steps to build, deploy and test tempus-hbase-compaction

##### Environment
Below steps are tested on a single node HDP 2.6.2.0-205 running HDFS, HBase and Phoenix on CentOS
 7.4. For installation of HDP on CentOS refer **https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.2/index.html**

##### Clone tempus-extensions and change directory to _tempus-hbase-compaction_ 
    $ git clone https://github.com/hashmapinc/tempus-extensions
    l$ cd tempus-extensions/tempus-hbase-compaction
##### Build Protobuf. The corresponding Java code will be created in _Utils/src/main/java/com/hashmapinc/tempus/CompactionProtos.java_
    $ ./Protobuf/gen-proto.sh
##### Build the Compaction client, HBase coprocessor, and Phoenix UDF
    $ mvn clean install -T2
##### Create tag_list, tag_data and tdc tables in Phoenix for testing 
    $ sqlline.py <zk-quorum>:2181 < Config/Phoenix/tag_list.sql
    $ sqlline.py <zk-quorum>:2181 < Config/Phoenix/tag_data.sql
    $ sqlline.py <zk-quorum>:2181 < Config/Phoenix/tdc.sql
##### Upsert sample data
    $ sqlline.py <zk-quorum>:2181 < Config/Phoenix/test-data/tag_list_sample.sql
    $ sqlline.py <zk-quorum>:2181 < Config/Phoenix/test-data/tag_data_sample.sql
##### Deploy the Coprocessor Jar to HBase region servers lib path. On a Single Node HDP, use the below command. For a production cluster with multiple region servers, copy the coprocessor jar to lib path of all the region servers
    $ cp CompactionService/target/uber-compaction-service-0.0.1-SNAPSHOT.jar /usr/hdp/current/hbase-regionserver/lib/
##### Add co-processor entry in hbase-site.xml
- Add the foll. key `hbase.coprocessor.region.classes` in hbase-site.xml if not present. The value
 will be full class name of our co-processor i.e `com.hashmapinc.tempus.CompactionEPC`. If other 
 values are already present add the new entry separated by a comma.
- Restart all the region servers and monitor for any errors 
##### We can now test the client with the sample data populated above
- Open _Config/Properties/compaction.properties_ and change the property **hbase.zookeeper.url** with value suitable to your configuration.
##### Run below command from _tempus-hbase-compaction_ directory. 
    $ ./CompactionClient/bin/run-compaction.sh 
    ./CompactionClient/target/uber-compaction-client-0.0.1-SNAPSHOT.jar com.hashmapinc.tempus.CompactionClient Config/Properties/compaction.properties Config/Properties/log4j.properties
##### To Uncompact and get the original data back [Ref: https://phoenix.apache.org/udf.html]
- Create a new directory in HDFS where the UDF will be copied. We will name it as `lib` and will 
be created as specified by the value of property `hbase.rootdir`. Eg: If `hbase.rootdir` value is
 _/apps/hbase/data/_, then the UDF has to be copied at _/apps/hbase/data/lib_
- Add foll new properties in hbase-site.xml 
    - Name: `phoenix.functions.allowUserDefinedFunctions`, Value: `true` 
    - Name: `hbase.dynamic.jars.dir`, Value: `${hbase.rootdir}/lib`
- Copy UDF to HDFS path specified by value of `hbase.dynamic.jars.dir`
- Run **CREATE FUNCTION** as shown in below commands 
sqlline.py
- Before executing the UDF below environment variables have to be exported else UDF will thrown a
 _RuntimeException_
    - `export PHOENIX_CONN_PARAM="jedireborn.net:2181:/hbase-unsecure"` - For performing certain DB queries in the UDF.
    - `export TAGLIST_TABLE=tag_list` - Table which stores the datatypes of URI.
    - Optional `export UNCOMPACT_TABLE=td_uncompact` - By default uncompacted data is stored in 
    _**tduc**_ table. This export makes it configurable.
- Execute the below query to test the UDF. Uncompacted data will be upserted to a either 
_**tduc**_ or as defined by env _**UNCOMPACT_TABLE**_
##### Commands for Uncompaction
    $ hadoop fs -copyFromLocal PhoenixUDFs/target/uber-uncompact-0.0.1-SNAPSHOT.jar 
    /apps/hbase/data/lib/uncompact.jar
    $ export PHOENIX_CONN_PARAM="localhost:2181:/hbase-unsecure"
    $ export TAGLIST_TABLE=tag_list
    $ sqlline> CREATE FUNCTION UNCOMPACT(VARBINARY, VARBINARY, VARBINARY, INTEGER, VARCHAR, VARCHAR, BIGINT) returns VARCHAR as 'com.hashmapinc.tempus.Uncompact' using jar 
    'hdfs://jedireborn.net:8020/apps/hbase/data/lib/uncompact.jar
    $ sqlline> select UNCOMPACT("VB", "Q", "TS", "NS", 'T1', 'T2', "ID")from tdc [where id = X | 
    where id = X and STTS <= T2 and STTS >= T1]; 
   





