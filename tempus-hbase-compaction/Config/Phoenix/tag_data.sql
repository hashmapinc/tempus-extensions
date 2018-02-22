DROP TABLE IF EXISTS tag_data;
--tag_data table stores values and quality emitted by a URI 
CREATE TABLE tag_data
(
	id BIGINT NOT NULL,		-- This is the uri to be used as an FK to the tag_list table	
	ts DATE NOT NULL, 		-- This is the timestamp from the Epoch, can be used forward or backward using Hbase reverse scan capability
	vl BIGINT,				-- Stores Long/Integer/Short data of a URI 
	vd DOUBLE,				-- Stores Float/Double data of URI
	vs VARCHAR,				-- Stores String data of URI
	q SMALLINT				-- This is the quality of the data
	CONSTRAINT pk PRIMARY KEY (id, ts ROW_TIMESTAMP)
) COMPRESSION='SNAPPY';