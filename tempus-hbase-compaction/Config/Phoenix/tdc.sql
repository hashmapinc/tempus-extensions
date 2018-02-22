DROP TABLE IF EXISTS tdc;
--This table is for storing all compacted URI's.
CREATE TABLE tdc
(       id BIGINT NOT NULL,	-- Compacted URI 
        stts DATE NOT NULL,      	-- First TS of the uncompacted record from 'tag_data' table for this URI
        vb VARBINARY,               -- Compacted Value
        q VARBINARY,                -- Compacted Data Quality
        ts VARBINARY,               -- Compacted TS
        ns INTEGER,         		-- Number of Compacted records
        upts DATE					-- when this record wa upserted
        CONSTRAINT PK PRIMARY KEY (id, stts)
) COMPRESSION='SNAPPY';