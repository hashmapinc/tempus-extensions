DROP TABLE IF EXISTS tag_data;
CREATE TABLE tag_data(id BIGINT NOT NULL, ts DATE NOT NULL, vl BIGINT, vd DOUBLE, vs VARCHAR, q SMALLINT CONSTRAINT pk PRIMARY KEY (id, ts ROW_TIMESTAMP)) COMPRESSION='SNAPPY'
