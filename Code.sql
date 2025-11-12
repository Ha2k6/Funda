-- ===========================
-- TASK 2 : FULL HIVE SOLUTION
-- ===========================

-- 1. CREATE INTERNAL TABLE
CREATE TABLE CovidDataWarehouse (
    location STRING,
    continent STRING,
    date_current STRING,
    total_cases BIGINT,
    total_deaths BIGINT,
    total_vaccinations BIGINT,
    diabetes_prevalence FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- LOAD DATA INTO TABLE
LOAD DATA INPATH '/user/hive/CovidGlobalData.csv'
INTO TABLE CovidDataWarehouse;

-- ===========================================
-- 2. VACCINATIONS COUNT PER LOCATION
-- ===========================================
SELECT location, SUM(total_vaccinations) AS total_vaccinations
FROM CovidDataWarehouse
GROUP BY location;

-- ===========================================
-- 3. LOCATIONS STARTING WITH "United.*"
-- ===========================================
SELECT location, total_vaccinations
FROM CovidDataWarehouse
WHERE location REGEXP '^United.*';

-- ===========================================
-- 4. CREATE PARTITIONED TABLE BY CONTINENT
-- ===========================================
CREATE TABLE CovidDataWarehouse_Partitioned (
    location STRING,
    date_current STRING,
    total_cases BIGINT,
    total_deaths BIGINT,
    total_vaccinations BIGINT,
    diabetes_prevalence FLOAT
)
PARTITIONED BY (continent STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- LOAD PARTITIONED DATA
INSERT INTO TABLE CovidDataWarehouse_Partitioned
PARTITION(continent)
SELECT 
    location,
    date_current,
    total_cases,
    total_deaths,
    total_vaccinations,
    diabetes_prevalence,
    continent
FROM CovidDataWarehouse;

-- ===========================================
-- 5. COUNT CONTINENTS + CREATE 4 BUCKETS
-- ===========================================

-- Count distinct continents
SELECT COUNT(DISTINCT continent) AS continent_count
FROM CovidDataWarehouse;

-- Create bucketed table
CREATE TABLE CovidDataWarehouse_Bucketed (
    location STRING,
    continent STRING,
    date_current STRING,
    total_cases BIGINT,
    total_deaths BIGINT,
    total_vaccinations BIGINT,
    diabetes_prevalence FLOAT
)
CLUSTERED BY (continent)
INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Insert all data into bucketed table
SET hive.enforce.bucketing = true;
SET hive.enforce.sorting = true;

INSERT INTO TABLE CovidDataWarehouse_Bucketed
SELECT * FROM CovidDataWarehouse;

-- ===========================================
-- 6. MAX, MIN, AVG INFECTED IN EACH BUCKET
-- ===========================================
SELECT 
    INPUT__FILE__NAME AS bucket_file,
    MAX(total_cases) AS max_infected,
    MIN(total_cases) AS min_infected,
    AVG(total_cases) AS avg_infected
FROM CovidDataWarehouse_Bucketed
GROUP BY INPUT__FILE__NAME;

-- ===========================================
-- 7. TOTAL DEATHS PER CONTINENT
-- ===========================================
SELECT continent, SUM(total_deaths) AS total_deaths
FROM CovidDataWarehouse
GROUP BY continent;

-- ===========================================
-- 8. AVG DIABETES PREVALENCE FOR ISRAEL
-- ===========================================
SELECT AVG(diabetes_prevalence) AS avg_diabetes_prevalence
FROM CovidDataWarehouse
WHERE location = 'Israel';
