DROP TABLE IF EXISTS tag_list;
--tag_list table stores attributes for a URI
CREATE TABLE tag_list
(
    id BIGINT PRIMARY KEY,    -- This is the CRC for the URI.
    name VARCHAR(50),         -- This is tag Name.
    uri VARCHAR(150),         -- URI is a string comprised of (/facility(id)/asset(id)/tag(id)).
    uuid VARCHAR(50),         -- This is uuid for tag.
    datatype VARCHAR(16),     -- datatype of the value for this URI.
    assetid VARCHAR,          -- This is uuid for asset for each tag.
    channelname VARCHAR,      -- Redundant field with "name" field. 
    channelid BIGINT,         -- Redundant field with "id" field. 
    source VARCHAR,           -- Source of tag data. 
    status SMALLINT,          -- status specifying active/inactive/delayed etc. states
    uom VARCHAR(32),          -- Unit of measure for tag.
    startdate DATE,           -- Start date.
    enddate DATE,             -- End date.
    description VARCHAR(50)   -- Description.
) COMPRESSION='SNAPPY';
