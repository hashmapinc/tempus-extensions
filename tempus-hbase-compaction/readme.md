# Steps to build, deploy and test tempus-hbase-compaction

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
##### Upsert test data
    $ sqlline.py <zk-quorum>:2181 < Config/Phoenix/test-data/tag_list_sample.sql
    $ sqlline.py <zk-quorum>:2181 < Config/Phoenix/test-data/tag_data_sample.sql
##### Deploy the Coprocessor Jar to HBase region servers lib path
- On a Standalone HDP 2.6, use the below command
    -  `$ cp CompactionService/target/uber-compaction-service-0.0.1-SNAPSHOT.jar /usr/hdp/current/hbase-regionserver/lib/`
- For a production cluster, copy the coprocessor jar to all the region servers
##### Add co-processor entry in hbase-site.xml
- Add the foll. key `hbase.coprocessor.region.classes` in hbase-site.xml if not present. The value
 will be full class name of our co-processor i.e `com.hashmapinc.tempus.CompactionEPC`. If other 
 values are already present add the new entry separated by a comma.
##### Restart HBase region servers
-  Restart all the region servers and monitor for any errors 
##### We can now test the client with the sample data populated above
- Open _Config/Properties/compaction.properties_ and change the property **hbase.zookeeper.url** with value suitable to your configuration.
- Below commands are executed in the directory _tempus-hbase-compaction_.
#####
    $# ln -s Config/Properties/compaction.properties
    $# ln -s Config/Properties/log4j.properties
    $ ./CompactionClient/bin/run-compaction.sh 
    ./CompactionClient/target/uber-compaction-client-0.0.1-SNAPSHOT.jar com.hashmapinc.tempus.CompactionClient Config/Properties/compaction.properties Config/Properties/log4j.properties
##### To Uncompact and get the original data back
- Copy UDF to HDFS
- Add UDF to Phoenix. Fire below query in sqlline.py
- Execute the below query to test the UDF. Uncompacted data will be upserted to a new table **_tduc_**
#####
    $ hadoop fs -copyFromLocal PhoenixUDFs/target /apps/hbase/data/lib
    $ sqlline> CREATE FUNCTION UNCOMPACT
    $ sqlline> select 




