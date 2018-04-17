# Steps to Build


##### Clone tempus-extensions
- $ _git clone https://<-git_username->@github.com/hashmapinc/tempus-extensions_
- $ _./Config/Protobuf/gen-proto.sh_
- $ _mvn clean install_


To Run the compaction client
Create tag_list, tag_data and tdc tables
sqlline.py <zk_quorum>:2181 < Config/Phoenix/tag_list.sql
sqlline.py <zk_quorum>:2181 < Config/Phoenix/tag_data.sql
sqlline.py <zk_quorum>:2181 < Config/Phoenix/tdc.sql

Upsert test data
sqlline.py <zk_quorum>:2181 < Config/Phoenix/test-data/tag_list_sample.sql
sqlline.py <zk_quorum>:2181 < Config/Phoenix/test-data/tag_data_sample.sql

Deploy the Coprocessor Jar

Run the Client
cd CompactionClient/bin
ln -s ../../Config/Properties/compaction.properties
ln -s ../../Config/Properties/log4j.properties
./run-compaction.sh ../target/uber-compaction-client-0.0.1-SNAPSHOT.jar com.hashmapinc.tempus.CompactionClient compaction.properties log4j.properties


To Uncomact and get the original data back
Build the UDF
mvn clean install

Copy UDF to HDFS
Add UDF to Phoenix

execute the below query to test



