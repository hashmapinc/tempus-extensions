# Steps to Build

##### Clone tempus-extensions and change directory to _tempus-hbase-compaction_ 
    - $ git clone https://github.com/hashmapinc/tempus-extensions
    - $ cd tempus-hbase-compaction
##### Build Protobuf
    - $ `./Config/Protobuf/gen-proto.sh`
##### Build the compaction client, HBase coprocessor, and Phoenix UDF
    - $ `mvn clean install`
##### Create tag_list, tag_data and tdc tables in Phoenix for testing 
    - $ `sqlline.py <zk-quorum>:2181 < Config/Phoenix/tag_list.sql`
    - $ `sqlline.py <zk-quorum>:2181 < Config/Phoenix/tag_data.sql`
    - $ `sqlline.py <zk-quorum>:2181 < Config/Phoenix/tdc.sql`
##### Upsert test data
    - $ `sqlline.py <zk-quorum>:2181 < Config/Phoenix/test-data/tag_list_sample.sql`
    - $ `sqlline.py <zk-quorum>:2181 < Config/Phoenix/test-data/tag_data_sample.sql`
##### Deploy the Coprocessor Jar to HBase region servers lib path
- On a Standalone HDP 2.6, use the below command
    - $ `cp CompactionService/target/ /usr/hdp/current/hbase-regionserver/lib/`
- For a production cluster, copy the coprocessor jar to all the region servers
##### Add co-processor entry in hbase-site.xml
- Add entry for key
##### Restart HBase regionservers
- On HDP 2.6 restart the affected services after making the above entry in hbase-site.xml
- For production cluster restart all the region servers and monitor for any errors 
#### We can now test the client with the sample data populated above  
- $ `ln -s ../../Config/Properties/compaction.properties`
- $ `ln -s ../../Config/Properties/log4j.properties`
- $ `./run-compaction.sh ../target/uber-compaction-client-0.0.1-SNAPSHOT.jar com.hashmapinc.tempus.CompactionClient compaction.properties log4j.properties`
##### To Uncompact and get the original data back
- Copy UDF to HDFS
    - $ `hadoop fs -copyFromLocal PhoenixUDFs/target /apps/hbase/data/lib`
- Add UDF to Phoenix. Fire below query in sqlline.py
    - sqlline> CREATE FUNCTION UNCOMPACT
- Execute the below query to test the UDF. Uncompacted data will be upserted to a new table **_tduc_**



