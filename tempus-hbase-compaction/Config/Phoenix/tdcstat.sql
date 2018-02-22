DROP TABLE IF EXISTS tdcstat;
--This table is for storing the last compaction TS.
CREATE TABLE tdcstat
(
        lcts DATE PRIMARY KEY,	-- TS when last compaction was run
        cmpnum BIGINT			-- Number of records compacted
) COMPRESSION='SNAPPY';
