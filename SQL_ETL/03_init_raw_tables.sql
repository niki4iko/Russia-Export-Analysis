/*
The purpose of the script is just to load raw .csv data into the tables
without any transformations, as this will happen later

Currently works only on my machine
(or if you place the repo exactly like in COPY)

TODO: fix it on later stages with /COPY instructions and relative paths
*/

-- ================== bronze.raw_partner_areas ==================
-- Drop table if exists and create as new
DROP TABLE IF EXISTS bronze.raw_partner_areas;
CREATE TABLE bronze.raw_partner_areas (
    country_key  SMALLINT,
    country_name VARCHAR(50)
);

-- Load CSV into the table
COPY bronze.raw_partner_areas
FROM 'A:\Git_repos\Russia-Export-Analysis\data_raw\partnerAreas.csv'
DELIMITER ','
CSV HEADER;


-- Preview first 10 rows
-- SELECT * FROM bronze.raw_partner_areas LIMIT 10;

-- ================== bronze.raw_iso3 ==================
DROP TABLE IF EXISTS bronze.raw_iso3;

CREATE TABLE bronze.raw_iso3 (
    continent_name VARCHAR(8), -- 8 cap
    region_name    VARCHAR(50), -- 17 cap
    country_name   VARCHAR(50), -- 33 cap
    capital_name   VARCHAR(50), -- 22 cap
    fips           CHAR(2),
    iso2           CHAR(2),
    iso3           CHAR(3),
    iso_key        VARCHAR(10),
    internet       VARCHAR(5),
    note           TEXT -- will be dropped later, not used in raw data
);


COPY bronze.raw_iso3
FROM 'A:\Git_repos\Russia-Export-Analysis\data_raw\iso3.csv'
DELIMITER ','
CSV HEADER;

-- Preview first 10 rows
-- SELECT * FROM bronze.raw_iso3 LIMIT 10;

-- ================== bronze.raw_rus_export ==================
DROP TABLE IF EXISTS bronze.raw_rus_export;

CREATE TABLE bronze.raw_rus_export (
    index_key        INTEGER,
    classification   VARCHAR(10), -- only "S4" rows, will be dropped
    year_num         SMALLINT,
    aggregate_level  SMALLINT,
    is_leaf_code     BOOLEAN,
    reporter_key     INTEGER,      -- Always "Russian Federation", so cut
    reporter_name    VARCHAR(100), -- same as above
    reporter_iso3    CHAR(3),      -- same as above
    partner_key      INTEGER,
    partner_name     VARCHAR(100),
    partner_iso3     CHAR(3),
    commodity_code   VARCHAR(5),  -- SITC 4 classification code, 
    commodity_name   VARCHAR(500), 
    qty_unit_key     SMALLINT,
    qty_unit_name    VARCHAR(50),
    qty_amount       NUMERIC, -- BIGINT
    net_weight_kg    NUMERIC, -- BIGINT
    trade_value_usd  NUMERIC(18,2)
);

COPY bronze.raw_rus_export
FROM 'A:\Git_repos\Russia-Export-Analysis\data_raw\RUStoWorldTrade.csv'
DELIMITER ','
CSV HEADER;

-- Preview first 1000 rows
-- SELECT * FROM bronze.raw_rus_export LIMIT 1000;
