DROP TABLE IF EXISTS tdc;
CREATE TABLE tdc(id BIGINT NOT NULL, stts DATE NOT NULL, vb VARBINARY, q VARBINARY, ts VARBINARY, ns
 INTEGER, upts DATE CONSTRAINT pk PRIMARY KEY (id, stts)) COMPRESSION='SNAPPY';
 