DROP TABLE IF EXISTS tag_list;
CREATE TABLE tag_list(id BIGINT PRIMARY KEY, name VARCHAR(50), uri VARCHAR(150), uuid VARCHAR(50), datatype VARCHAR(16),
assetid VARCHAR, channelname VARCHAR, channelid BIGINT, source VARCHAR, status SMALLINT, uom VARCHAR(32), startdate DATE, enddate DATE, description VARCHAR(50) ) COMPRESSION='SNAPPY'
