/*
Purpose: Profile raw tables to determine field boundaries,
check for NULLs, duplicates. No transformation should happen here.

Currently considering it incomplete and somewhere messy
The content of this file is still subject to change as my skills will grow
*/

-- ========================= raw_partner_areas =========================
-- Check for NULL-key and doubles
SELECT
	country_key,
	COUNT(*) as row_total
FROM bronze.raw_partner_areas
GROUP BY country_key
HAVING COUNT(*) > 1 

-- Check string lengths and nulls
SELECT 
	COUNT(*)                       AS total_rows,    -- 292
	MIN(country_key)               AS min_key,       -- 4
    MAX(country_key)               AS max_key,       -- 899
	COUNT(country_key)             AS key_non_null,  -- 292
    COUNT(*) - COUNT(country_key)  AS key_non_nulls, -- 0
    MIN(LENGTH(country_name))      AS min_len,       -- 3
    MAX(LENGTH(country_name))      AS max_len,       -- 44
    COUNT(country_name)            AS name_non_null, -- 292
    COUNT(*) - COUNT(country_name) AS name_nulls     -- 0
FROM bronze.raw_partner_areas;

-- ============================== raw_iso3 ==============================
-- Profile text fields
SELECT 
    MAX(LENGTH(continent_name)) AS max_len_continent, -- 8
    MAX(LENGTH(region_name))    AS max_len_region,    -- 17
    MAX(LENGTH(country_name))   AS max_len_country,   -- 33
    MAX(LENGTH(capital_name))   AS max_len_capital,   -- 22
    MAX(LENGTH(fips))           AS max_len_fips,      -- 2
    MAX(LENGTH(iso2))           AS max_len_iso2,      -- 2
    MAX(LENGTH(iso3))           AS max_len_iso3,      -- 3
    MAX(LENGTH(internet))       AS max_len_internet,  -- 5
    MAX(LENGTH(note))           AS max_len_note       -- 1
FROM bronze.raw_iso3;

-- Profile numeric ISO key
SELECT 
    MIN(iso_key::INT) AS min_iso_key,  -- 4
    MAX(iso_key::INT) AS max_iso_key,  -- 894
    COUNT(*) AS total_rows,            -- 226
    COUNT(iso_key) AS non_null,        -- 226
    COUNT(*) - COUNT(iso_key) AS nulls -- 0
FROM bronze.raw_iso3
WHERE iso_key ~ '^[0-9]+$'; -- only numeric values

-- =========================== raw_rus_export ===========================
-- Max string len
SELECT 
    MAX(LENGTH(classification)) AS max_len_classification, -- 2
    MAX(LENGTH(reporter_name))  AS max_len_reporter_name,  -- 18
    MAX(LENGTH(reporter_iso3))  AS max_len_reporter_iso3,  -- 3
    MAX(LENGTH(partner_name))   AS max_len_partner_name,   -- 36
    MAX(LENGTH(partner_iso3))   AS max_len_partner_iso3,   -- 3
    MAX(LENGTH(commodity_code)) AS max_len_commodity_code, -- 5
    MAX(LENGTH(commodity_name)) AS max_len_commodity_name, -- 411
    MAX(LENGTH(qty_unit_name))  AS max_len_qty_unit_name   -- 48
FROM bronze.raw_rus_export;
--  Numeric range profiling 
SELECT 
    MIN(year_num)        AS min_year, -- 2007
    MAX(year_num)        AS max_year, -- 2020
    MIN(aggregate_level) AS min_aggregate_level, -- 0
    MAX(aggregate_level) AS max_aggregate_level, -- 5
    MIN(qty_amount)      AS min_qty_amount,  -- 0
    MAX(qty_amount)      AS max_qty_amount,  -- 75,265,886,900 
    MIN(net_weight_kg)   AS min_net_weight,  -- 0
    MAX(net_weight_kg)   AS max_net_weight,  -- 75,265,886,900
    MIN(trade_value_usd) AS min_trade_value, -- 0.00
    MAX(trade_value_usd) AS max_trade_value  -- 66,607,214,414.00 
FROM bronze.raw_rus_export;
-- Null counts
SELECT 
    COUNT(*)                          AS total_rows, -- 1,384,603
    COUNT(classification)             AS non_null_classification, -- 1,384,603
    COUNT(*) - COUNT(classification)  AS null_classification,     -- 0
    COUNT(reporter_name)              AS non_null_reporter_name,  -- 1,384,603
    COUNT(*) - COUNT(reporter_name)   AS null_reporter_name,      -- 0
    COUNT(partner_name)               AS non_null_partner_name,   -- 1,384,603
    COUNT(*) - COUNT(partner_name)    AS null_partner_name,       -- 0
    COUNT(commodity_code)             AS non_null_commodity_code, -- 1,384,603
    COUNT(*) - COUNT(commodity_code)  AS null_commodity_code,     -- 0
    COUNT(commodity_name)             AS non_null_commodity_name, -- 1,384,603
    COUNT(*) - COUNT(commodity_name)  AS null_commodity_name,     -- 0
    COUNT(qty_amount)                 AS non_null_qty_amount,     -- 1,283,894
    COUNT(*) - COUNT(qty_amount)      AS null_qty_amount,         -- 100,709
	COUNT(qty_unit_key)               AS non_null_qty_units,      -- 1,384,603
	COUNT(*) - COUNT(qty_unit_key)    AS null_qty_units,          -- 0
    COUNT(net_weight_kg)              AS non_null_net_weight,     -- 1,323,567
    COUNT(*) - COUNT(net_weight_kg)   AS null_net_weight,         -- 61,036
    COUNT(trade_value_usd)            AS non_null_trade_value,    -- 1,384,603
    COUNT(*) - COUNT(trade_value_usd) AS null_trade_value         -- 0
FROM bronze.raw_rus_export;
-- Distinct counts
SELECT 
    COUNT(DISTINCT classification)  AS distinct_classification,   -- 1
    COUNT(DISTINCT year_num)        AS distinct_years,            -- 14
    COUNT(DISTINCT aggregate_level) AS distinct_aggregate_levels, -- 6
    COUNT(DISTINCT is_leaf_code)    AS distinct_leaf_flags,       -- 2
    COUNT(DISTINCT reporter_key)    AS distinct_reporters,        -- 1
    COUNT(DISTINCT reporter_name)   AS distinct_reporter_names,   -- 1
    COUNT(DISTINCT reporter_iso3)   AS distinct_reporter_iso3,    -- 1
    COUNT(DISTINCT partner_key)     AS distinct_partners,         -- 225
    COUNT(DISTINCT partner_name)    AS distinct_partner_names,    -- 225
    COUNT(DISTINCT partner_iso3)    AS distinct_partner_iso3,     -- 222
    COUNT(DISTINCT commodity_code)  AS distinct_commodity_codes,  -- 3979
    COUNT(DISTINCT commodity_name)  AS distinct_commodity_names,  -- 3938
    COUNT(DISTINCT qty_unit_key)    AS distinct_qty_units,        -- 12
    COUNT(DISTINCT qty_unit_name)   AS distinct_qty_unit_names    -- 12
FROM bronze.raw_rus_export;
-- Top values preview
SELECT qty_unit_name, COUNT(*) AS row_total
FROM bronze.raw_rus_export
GROUP BY qty_unit_name
ORDER BY row_total DESC
LIMIT 15; -- should be only 12 possible rows, cause distinct_qty_unit_names = 12



/*
All commodity_code data is under SITC 4 classification rules. It means
If LEN(commodity_code) is as follows, then:
1‑digit (Sections) → very broad categories (e.g., 0 = Food and live animals).
2‑digit (Divisions) → narrower groups (e.g., 01 = Meat and meat preparations).
3‑digit (Groups) → more detail (e.g., 011 = Meat of bovine animals).
4‑digit (Subgroups) → finer detail (e.g., 0111 = Fresh/chilled bovine meat).
5‑digit (Items) → the most detailed commodity codes
Also in this dataset commodity_code may be TOTAL, 
which means all kinds of commodities

aggregate_level MUST always be equal to LEN(commodity_code),
except cases where aggreagate_level is 0, then LEN(commodity_code) MUST be 5,
the value MUST be "TOTAL" and is_leaf_code MUST be false

If is_leaf_code is set, then LEN(commodity_code) ideally should be 5
but there is a SITC 4 classifications that has max len of only 4, 
	so i consider it ok data
but if is_leaf_code == True and LEN(commodity_code) < 4, 
	then considering it suspicious

credits to Microsoft Copilot who helped me understand the concept

Having that said, following code shows samples which does NOT qualify
expected output is empty table
*/
SELECT
	is_leaf_code AS leaf_summary,
	aggregate_level,
	LENGTH(commodity_code) AS code_length,
	COUNT(commodity_code)  AS matching_commodities, 
	COUNT(aggregate_level) AS matching_aggregate
FROM bronze.raw_rus_export
GROUP BY is_leaf_code, aggregate_level, LENGTH(commodity_code)
HAVING NOT (
	aggregate_level = LENGTH(commodity_code)
	OR (aggregate_level = 0 AND LENGTH(commodity_code) = 5 AND is_leaf_code = FALSE)
	OR (is_leaf_code = TRUE AND LENGTH(commodity_code) < 4)
)
ORDER BY is_leaf_code, aggregate_level, code_length

/* Just to see the data sample
SELECT
	is_leaf_code,
	commodity_code,
	aggregate_level,
	commodity_name
FROM bronze.raw_rus_export
LIMIT 100;
*/