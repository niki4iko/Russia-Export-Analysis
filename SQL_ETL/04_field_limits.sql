/*
Purpose: Profile raw tables to determine field limits (lengths, min/max values, null counts).
This helps decide appropriate data types for warehouse tables.
*/

-- ========================= raw_partner_areas =========================
-- Check string lengths and nulls
SELECT 
	COUNT(*) AS total_rows,
	MIN(country_key) AS min_key,
    MAX(country_key) AS max_key,
	COUNT(country_key) AS non_null,
    COUNT(*) - COUNT(country_key) AS nulls,
    MIN(LENGTH(country_name)) AS min_len,
    MAX(LENGTH(country_name)) AS max_len,
    COUNT(country_name) AS non_null,
    COUNT(*) - COUNT(country_name) AS nulls
FROM stage.raw_partner_areas;

-- ============================== raw_iso3 ==============================
-- Profile text fields
SELECT 
    MAX(LENGTH(continent_name)) AS max_len_continent,
    MAX(LENGTH(region_name)) AS max_len_region,
    MAX(LENGTH(country_name)) AS max_len_country,
    MAX(LENGTH(capital_name)) AS max_len_capital,
    MAX(LENGTH(fips)) AS max_len_fips,
    MAX(LENGTH(iso2)) AS max_len_iso2,
    MAX(LENGTH(iso3)) AS max_len_iso3,
    MAX(LENGTH(internet)) AS max_len_internet,
    MAX(LENGTH(note)) AS max_len_note
FROM stage.raw_iso3;

-- Profile numeric ISO key
SELECT 
    MIN(iso_key::INT) AS min_iso_key,
    MAX(iso_key::INT) AS max_iso_key,
    COUNT(*) AS total_rows,
    COUNT(iso_key) AS non_null,
    COUNT(*) - COUNT(iso_key) AS nulls
FROM stage.raw_iso3
WHERE iso_key ~ '^[0-9]+$';  -- only numeric values

-- =========================== raw_rus_export ===========================
-- Max string len
SELECT 
    MAX(LENGTH(classification))   AS max_len_classification, -- 2
    MAX(LENGTH(reporter_name))    AS max_len_reporter_name, -- 18
    MAX(LENGTH(reporter_iso3))    AS max_len_reporter_iso3, -- 3
    MAX(LENGTH(partner_name))     AS max_len_partner_name, -- 36
    MAX(LENGTH(partner_iso3))     AS max_len_partner_iso3, -- 3
    MAX(LENGTH(commodity_key))    AS max_len_commodity_key, -- 5
    MAX(LENGTH(commodity_name))   AS max_len_commodity_name, -- 411
    MAX(LENGTH(qty_unit_name))    AS max_len_qty_unit_name -- 48
FROM stage.raw_rus_export;
--  Numeric range profiling 
SELECT 
    MIN(year_num)        AS min_year, -- 2007
    MAX(year_num)        AS max_year, -- 2020
    MIN(aggregate_level) AS min_aggregate_level, -- 0
    MAX(aggregate_level) AS max_aggregate_level, -- 5
    MIN(qty_amount)      AS min_qty_amount, -- 0
    MAX(qty_amount)      AS max_qty_amount, -- 75,265,886,900 
    MIN(net_weight_kg)   AS min_net_weight, -- 0
    MAX(net_weight_kg)   AS max_net_weight, -- 75,265,886,900
    MIN(trade_value_usd) AS min_trade_value, -- 0.00
    MAX(trade_value_usd) AS max_trade_value -- 66,607,214,414.00 
FROM stage.raw_rus_export;
-- Null counts
SELECT 
    COUNT(*) AS total_rows, -- 1,384,603
    COUNT(classification)   AS non_null_classification, -- 1,384,603
    COUNT(*) - COUNT(classification)   AS null_classification, -- 0
    COUNT(reporter_name)    AS non_null_reporter_name, -- 1,384,603
    COUNT(*) - COUNT(reporter_name)    AS null_reporter_name, -- 0
    COUNT(partner_name)     AS non_null_partner_name, -- 1,384,603
    COUNT(*) - COUNT(partner_name)     AS null_partner_name, -- 0
    COUNT(commodity_key)    AS non_null_commodity_key, -- 1,384,603
    COUNT(*) - COUNT(commodity_key)    AS null_commodity_key, -- 0
    COUNT(commodity_name)   AS non_null_commodity_name, -- 1,384,603
    COUNT(*) - COUNT(commodity_name)   AS null_commodity_name, -- 0
    COUNT(qty_amount)       AS non_null_qty_amount, -- 1,283,894
    COUNT(*) - COUNT(qty_amount)       AS null_qty_amount, -- 100,709
	COUNT(qty_unit_key)     AS non_null_qty_units, -- -- 1,384,603
	COUNT(*) - COUNT(qty_unit_key)       AS null_qty_units, -- 0
    COUNT(net_weight_kg)    AS non_null_net_weight, -- 1,323,567
    COUNT(*) - COUNT(net_weight_kg)    AS null_net_weight, -- 61,036
    COUNT(trade_value_usd)  AS non_null_trade_value,  -- 1,384,603
    COUNT(*) - COUNT(trade_value_usd)  AS null_trade_value -- 0
FROM stage.raw_rus_export;
-- Distinct counts
SELECT 
    COUNT(DISTINCT classification)   AS distinct_classification, -- 1
    COUNT(DISTINCT year_num)         AS distinct_years, -- 14
    COUNT(DISTINCT aggregate_level)  AS distinct_aggregate_levels, -- 6
    COUNT(DISTINCT is_leaf_code)     AS distinct_leaf_flags, -- 2
    COUNT(DISTINCT reporter_key)     AS distinct_reporters, -- 1
    COUNT(DISTINCT reporter_name)    AS distinct_reporter_names, -- 1
    COUNT(DISTINCT reporter_iso3)    AS distinct_reporter_iso3, -- 1
    COUNT(DISTINCT partner_key)      AS distinct_partners, -- 225
    COUNT(DISTINCT partner_name)     AS distinct_partner_names, -- 225
    COUNT(DISTINCT partner_iso3)     AS distinct_partner_iso3, -- 222
    COUNT(DISTINCT commodity_key)    AS distinct_commodity_keys, -- 3979
    COUNT(DISTINCT commodity_name)   AS distinct_commodity_names, -- 3938
    COUNT(DISTINCT qty_unit_key)     AS distinct_qty_units, -- 12
    COUNT(DISTINCT qty_unit_name)    AS distinct_qty_unit_names -- 12
FROM stage.raw_rus_export;
-- Top values preview
SELECT qty_unit_name, COUNT(*) AS row_count
FROM stage.raw_rus_export
GROUP BY qty_unit_name
ORDER BY row_count DESC
LIMIT 15; -- there is only 12 possible rows as distinct qty_units = qty_unit_names = 12

SELECT
	AVG(commodity_key::INT),
	commodity_name, 
	COUNT(*) AS row_total
FROM stage.raw_rus_export
GROUP BY commodity_name
ORDER BY row_total DESC
LIMIT 100;

-- Where commodity_key is not an integer
SELECT COUNT(*) -- 177, all "TOTAL"
FROM stage.raw_rus_export
WHERE NOT (commodity_key ~ '^[0-9]+$');
-- Where commodity_key is an integer
SELECT COUNT(*) -- 1,384,426
FROM stage.raw_rus_export
WHERE (commodity_key ~ '^[0-9]+$');
-- total row count is 1,384,603 which is total row count in raw_rus_export