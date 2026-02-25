
-- ============================================================
-- 0) CLEAN START (rerunnable)
-- ============================================================
DROP SCHEMA IF EXISTS stg CASCADE;

DROP TABLE IF EXISTS fact_batch_quality_sustainability CASCADE;
DROP TABLE IF EXISTS batch_dim CASCADE;
DROP TABLE IF EXISTS plant_dim CASCADE;
DROP TABLE IF EXISTS product_dim CASCADE;
DROP TABLE IF EXISTS date_dim CASCADE;

CREATE SCHEMA stg;

-- ============================================================
-- 1) STAGING RAW (store everything as TEXT to avoid COPY issues)
-- ============================================================
CREATE TABLE stg.batch_data_raw (
  production_date            TEXT,
  batch_number               TEXT,
  shift                      TEXT,
  line_number                TEXT,

  plant_name                 TEXT,
  city                       TEXT,
  country                    TEXT,
  region                     TEXT,
  continent                  TEXT,

  sku_code                   TEXT,
  product_name               TEXT,
  product_line               TEXT,
  category                   TEXT,
  packaging_type             TEXT,

  units_produced             TEXT,
  units_rejected             TEXT,
  quality_test_score         TEXT,
  carbon_emissions_kg_co2e   TEXT,
  waste_kg                   TEXT,
  energy_kwh                 TEXT,
  water_liters               TEXT
);

-- ============================================================
-- 2) LOAD DIRTY CSV INTO STAGING RAW

COPY stg.batch_data_raw
FROM '/tmp/dirty_data_generated.csv'
WITH (FORMAT csv, HEADER true);

-- ============================================================
-- 3) CLEAN + TRANSFORM INTO STAGING CLEAN
--    Goals:
--    - normalize text (trim/case)
--    - standardize known variants (plants/countries/regions/continent)
--    - safely parse numbers (handle negatives/outliers)
--    - deduplicate at the grain
--    - produce NO NULLS (since you want final clean with no NULLs)
-- ============================================================

DROP TABLE IF EXISTS stg.batch_data_clean;

CREATE TABLE stg.batch_data_clean AS
WITH base AS (
  SELECT
    /* ---------- DATE ---------- */
    -- Generator outputs YYYY-MM-DD, but keep safe parsing
    CASE
      WHEN TRIM(production_date) ~ '^\d{4}-\d{2}-\d{2}$'
      THEN TRIM(production_date)::date
      ELSE DATE '2000-01-01'
    END AS production_date,

    /* ---------- CORE KEYS ---------- */
    COALESCE(NULLIF(TRIM(batch_number), ''), 'B-UNKNOWN') AS batch_number,
    COALESCE(NULLIF(UPPER(TRIM(shift)), ''), 'A') AS shift,
    COALESCE(NULLIF(UPPER(TRIM(line_number)), ''), 'L1') AS line_number,

    /* ---------- PLANT NORMALIZATION (matches your generator plants) ---------- */
    CASE
      WHEN LOWER(TRIM(plant_name)) LIKE '%bolzano%' THEN 'Plant Bolzano'
      WHEN LOWER(TRIM(plant_name)) LIKE '%vienna%'  THEN 'Plant Vienna'
      WHEN LOWER(TRIM(plant_name)) LIKE '%chicago%' THEN 'Plant Chicago'
      WHEN LOWER(TRIM(plant_name)) LIKE '%munich%'  THEN 'Plant Munich'
      ELSE COALESCE(NULLIF(INITCAP(TRIM(plant_name)), ''), 'Unknown Plant')
    END AS plant_name,

    COALESCE(NULLIF(INITCAP(TRIM(city)), ''), 'Unknown City') AS city,

    /* ---------- COUNTRY NORMALIZATION ---------- */
    COALESCE(
      CASE
        WHEN LOWER(TRIM(country)) IN ('italy','italia') THEN 'Italy'
        WHEN LOWER(TRIM(country)) IN ('austria','österreich','osterreich') THEN 'Austria'
        WHEN LOWER(TRIM(country)) IN ('usa','us','u.s.a','united states','united states of america') THEN 'USA'
        WHEN LOWER(TRIM(country)) IN ('germany','deutschland') THEN 'Germany'
        ELSE NULLIF(INITCAP(TRIM(country)), '')
      END,
      'Unknown Country'
    ) AS country,

    /* ---------- REGION NORMALIZATION (based on REGION_NOISE in generator) ---------- */
    COALESCE(
      CASE
        WHEN LOWER(TRIM(region)) IN ('bayern','bavaria','bavaria ') THEN 'Bavaria'
        WHEN LOWER(TRIM(region)) IN ('südtirol','sudtirol','southtyrol','south tyrol','south tyrol ') THEN 'South Tyrol'
        WHEN LOWER(TRIM(region)) IN ('mid-west','mid west','midwest') THEN 'Midwest'
        WHEN LOWER(TRIM(region)) IN ('east','east ','eastern') THEN 'East'
        ELSE NULLIF(INITCAP(TRIM(region)), '')
      END,
      'Unknown Region'
    ) AS region,

    /* ---------- CONTINENT NORMALIZATION ---------- */
    COALESCE(
      CASE
        WHEN LOWER(TRIM(continent)) LIKE '%north%' THEN 'North America'
        WHEN LOWER(TRIM(continent)) LIKE '%europe%' THEN 'Europe'
        ELSE NULLIF(INITCAP(TRIM(continent)), '')
      END,
      'Unknown Continent'
    ) AS continent,

    /* ---------- PRODUCT KEYS ---------- */
    COALESCE(NULLIF(UPPER(TRIM(sku_code)), ''), 'UNKNOWN_SKU') AS sku_code,
    COALESCE(NULLIF(INITCAP(TRIM(product_name)), ''), 'Unknown Product') AS product_name,
    COALESCE(NULLIF(INITCAP(TRIM(product_line)), ''), 'Unknown Line') AS product_line,
    COALESCE(NULLIF(INITCAP(TRIM(category)), ''), 'Unknown Category') AS category,
    COALESCE(NULLIF(INITCAP(TRIM(packaging_type)), ''), 'Unknown Packaging') AS packaging_type,

    /* ---------- SAFE NUMERIC PARSING ----------
       Generator stores numbers as strings; outliers can be negative or impossible.
       We also accept comma decimals just in case.
    */
    CASE
      WHEN TRIM(units_produced) ~ '^-?\d+$' THEN TRIM(units_produced)::int
      ELSE 0
    END AS units_produced_raw,

    CASE
      WHEN TRIM(units_rejected) ~ '^-?\d+$' THEN TRIM(units_rejected)::int
      ELSE 0
    END AS units_rejected_raw,

    CASE
      WHEN REPLACE(TRIM(quality_test_score), ',', '.') ~ '^-?\d+(\.\d+)?$'
      THEN REPLACE(TRIM(quality_test_score), ',', '.')::numeric(10,4)
      ELSE 0
    END AS quality_test_score_raw,

    CASE
      WHEN REPLACE(TRIM(carbon_emissions_kg_co2e), ',', '.') ~ '^-?\d+(\.\d+)?$'
      THEN REPLACE(TRIM(carbon_emissions_kg_co2e), ',', '.')::numeric(18,6)
      ELSE 0
    END AS carbon_emissions_kg_co2e_raw,

    CASE
      WHEN REPLACE(TRIM(waste_kg), ',', '.') ~ '^-?\d+(\.\d+)?$'
      THEN REPLACE(TRIM(waste_kg), ',', '.')::numeric(18,6)
      ELSE 0
    END AS waste_kg_raw,

    CASE
      WHEN REPLACE(TRIM(energy_kwh), ',', '.') ~ '^-?\d+(\.\d+)?$'
      THEN REPLACE(TRIM(energy_kwh), ',', '.')::numeric(18,6)
      ELSE 0
    END AS energy_kwh_raw,

    CASE
      WHEN REPLACE(TRIM(water_liters), ',', '.') ~ '^-?\d+(\.\d+)?$'
      THEN REPLACE(TRIM(water_liters), ',', '.')::numeric(18,6)
      ELSE 0
    END AS water_liters_raw

  FROM stg.batch_data_raw
),
fixed AS (
  SELECT
    production_date,
    (EXTRACT(YEAR FROM production_date)::int * 10000
     + EXTRACT(MONTH FROM production_date)::int * 100
     + EXTRACT(DAY FROM production_date)::int) AS date_id,

    batch_number, shift, line_number,
    plant_name, city, country, region, continent,
    sku_code, product_name, product_line, category, packaging_type,

    /* Enforce "no NULLs" and fix dirty numeric outliers */
    CASE WHEN units_produced_raw < 0 THEN 0 ELSE units_produced_raw END AS units_produced,

    CASE
      WHEN units_rejected_raw < 0 THEN 0
      WHEN units_rejected_raw > (CASE WHEN units_produced_raw < 0 THEN 0 ELSE units_produced_raw END)
      THEN (CASE WHEN units_produced_raw < 0 THEN 0 ELSE units_produced_raw END)
      ELSE units_rejected_raw
    END AS units_rejected,

    CASE
      WHEN quality_test_score_raw < 0 THEN 0
      WHEN quality_test_score_raw > 100 THEN 100
      ELSE quality_test_score_raw
    END AS quality_test_score,

    CASE WHEN carbon_emissions_kg_co2e_raw < 0 THEN 0 ELSE carbon_emissions_kg_co2e_raw END AS carbon_emissions_kg_co2e,
    CASE WHEN waste_kg_raw < 0 THEN 0 ELSE waste_kg_raw END AS waste_kg,
    CASE WHEN energy_kwh_raw < 0 THEN 0 ELSE energy_kwh_raw END AS energy_kwh,
    CASE WHEN water_liters_raw < 0 THEN 0 ELSE water_liters_raw END AS water_liters
  FROM base
),
dedup AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY production_date, batch_number, plant_name, sku_code, shift, line_number
        ORDER BY production_date
      ) AS rn
    FROM fixed
  ) t
  WHERE rn = 1
)
SELECT
  production_date, date_id,
  batch_number, shift, line_number,
  plant_name, city, country, region, continent,
  sku_code, product_name, product_line, category, packaging_type,
  units_produced, units_rejected, quality_test_score,
  carbon_emissions_kg_co2e, waste_kg, energy_kwh, water_liters
FROM dedup;

-- Optional indexes for faster loading
CREATE INDEX IF NOT EXISTS idx_clean_date  ON stg.batch_data_clean(date_id);
CREATE INDEX IF NOT EXISTS idx_clean_plant ON stg.batch_data_clean(plant_name, city, country);
CREATE INDEX IF NOT EXISTS idx_clean_sku   ON stg.batch_data_clean(sku_code);
CREATE INDEX IF NOT EXISTS idx_clean_batch ON stg.batch_data_clean(batch_number);

-- ============================================================
-- 4) CREATE DW TABLES (Star Schema)
-- ============================================================

CREATE TABLE date_dim (
  date_id     INT PRIMARY KEY,
  full_date   DATE NOT NULL,
  day         INT NOT NULL,
  week        INT NOT NULL,
  month       INT NOT NULL,
  quarter     INT NOT NULL,
  year        INT NOT NULL,
  season      VARCHAR(20) NOT NULL
);

CREATE TABLE product_dim (
  product_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku_code        VARCHAR(50) NOT NULL UNIQUE,
  product_name    VARCHAR(255) NOT NULL,
  product_line    VARCHAR(100) NOT NULL,
  category        VARCHAR(100) NOT NULL,
  packaging_type  VARCHAR(100) NOT NULL
);

CREATE TABLE plant_dim (
  plant_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  plant_name   VARCHAR(255) NOT NULL,
  city         VARCHAR(100) NOT NULL,
  country      VARCHAR(100) NOT NULL,
  region       VARCHAR(100) NOT NULL,
  continent    VARCHAR(100) NOT NULL,
  UNIQUE (plant_name, city, country)
);

CREATE TABLE batch_dim (
  batch_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  batch_number  VARCHAR(80) NOT NULL UNIQUE,
  shift         VARCHAR(20) NOT NULL,
  line_number   VARCHAR(50) NOT NULL
);

CREATE TABLE fact_batch_quality_sustainability (
  batch_id       BIGINT NOT NULL REFERENCES batch_dim(batch_id),
  plant_id       BIGINT NOT NULL REFERENCES plant_dim(plant_id),
  product_id     BIGINT NOT NULL REFERENCES product_dim(product_id),
  date_id        INT    NOT NULL REFERENCES date_dim(date_id),

  units_produced            INT NOT NULL,
  units_rejected            INT NOT NULL,
  quality_test_score        NUMERIC(10,4) NOT NULL,
  carbon_emissions_kg_co2e  NUMERIC(18,6) NOT NULL,
  waste_kg                  NUMERIC(18,6) NOT NULL,
  energy_kwh                NUMERIC(18,6) NOT NULL,
  water_liters              NUMERIC(18,6) NOT NULL,

  PRIMARY KEY (batch_id, plant_id, product_id, date_id)
);


CREATE INDEX idx_fact_date    ON fact_batch_quality_sustainability(date_id);
CREATE INDEX idx_fact_product ON fact_batch_quality_sustainability(product_id);
CREATE INDEX idx_fact_plant   ON fact_batch_quality_sustainability(plant_id);
CREATE INDEX idx_fact_batch   ON fact_batch_quality_sustainability(batch_id);

-- ============================================================
-- 5) LOAD DIMENSIONS FROM CLEAN STAGING
-- ============================================================

-- Date dim
INSERT INTO date_dim (date_id, full_date, day, week, month, quarter, year, season)
SELECT DISTINCT
  c.date_id,
  c.production_date,
  EXTRACT(DAY FROM c.production_date)::int,
  EXTRACT(WEEK FROM c.production_date)::int,
  EXTRACT(MONTH FROM c.production_date)::int,
  EXTRACT(QUARTER FROM c.production_date)::int,
  EXTRACT(YEAR FROM c.production_date)::int,
  CASE
    WHEN EXTRACT(MONTH FROM c.production_date)::int IN (12,1,2) THEN 'Winter'
    WHEN EXTRACT(MONTH FROM c.production_date)::int IN (3,4,5)  THEN 'Spring'
    WHEN EXTRACT(MONTH FROM c.production_date)::int IN (6,7,8)  THEN 'Summer'
    ELSE 'Autumn'
  END
FROM stg.batch_data_clean c
ON CONFLICT (date_id) DO NOTHING;

-- Product dim
INSERT INTO product_dim (sku_code, product_name, product_line, category, packaging_type)
SELECT DISTINCT
  c.sku_code,
  c.product_name,
  c.product_line,
  c.category,
  c.packaging_type
FROM stg.batch_data_clean c
ON CONFLICT (sku_code) DO NOTHING;

-- Plant dim
INSERT INTO plant_dim (plant_name, city, country, region, continent)
SELECT DISTINCT
  c.plant_name,
  c.city,
  c.country,
  c.region,
  c.continent
FROM stg.batch_data_clean c
ON CONFLICT (plant_name, city, country) DO NOTHING;

-- Batch dim
INSERT INTO batch_dim (batch_number, shift, line_number)
SELECT DISTINCT
  c.batch_number,
  c.shift,
  c.line_number
FROM stg.batch_data_clean c
ON CONFLICT (batch_number) DO NOTHING;

-- ============================================================
-- 6) LOAD FACT (join clean staging to dimensions)
-- ============================================================
INSERT INTO fact_batch_quality_sustainability (
  batch_id, plant_id, product_id, date_id,
  units_produced, units_rejected, quality_test_score,
  carbon_emissions_kg_co2e, waste_kg, energy_kwh, water_liters
)
SELECT
  b.batch_id,
  pl.plant_id,
  pr.product_id,
  c.date_id,
  c.units_produced,
  c.units_rejected,
  c.quality_test_score,
  c.carbon_emissions_kg_co2e,
  c.waste_kg,
  c.energy_kwh,
  c.water_liters
FROM stg.batch_data_clean c
JOIN batch_dim   b  ON b.batch_number = c.batch_number
JOIN product_dim pr ON pr.sku_code     = c.sku_code
JOIN plant_dim   pl ON pl.plant_name   = c.plant_name
                    AND pl.city        = c.city
                    AND pl.country     = c.country
ON CONFLICT (batch_id, plant_id, product_id, date_id) DO NOTHING;
