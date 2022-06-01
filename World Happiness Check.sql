/*
Title: World Happiness EDA
Author: Matthew Myers
Database Resource: https://www.kaggle.com/unsdsn/world-happiness
*/

/* OBJECTIVE:
To explore and clean the data from the World Happiness studies for 2015 - 2019 with the purpose of importing into Tableau.
The data collected for one year is often in a different format for various years from 2015 to 2019. For example, region 
not being included for some years as well as columns having different names.
*/

/* SECTION 1: Look at data */
-- 2015 Data
SELECT *
FROM WH2015;

-- 2016 Data
SELECT *
FROM WH2016;

-- 2017 Data
SELECT *
FROM WH2017;

-- 2018 Data
SELECT *
FROM WH2018;

-- 2019 Data
SELECT *
FROM WH2019;

/* SECTION 2: Data Cleaning */
-- Drop Duplicates From WH2016 (taken from https://docs.microsoft.com/en-us/troubleshoot/sql/database-design/remove-duplicate-rows-sql-server-tab)
-- Try method 2 because there isn't DISTINCT *
DELETE T
FROM
(
SELECT *
, DupRank = ROW_NUMBER() OVER (
              PARTITION BY Country
              ORDER BY (SELECT NULL)
            )
FROM WH2016
) AS T
WHERE DupRank > 1;

-- Add Column for Study Years to keep data straight while combining tables
ALTER TABLE WH2015
ADD study_year INTEGER NOT NULL DEFAULT 2015; -- For 2015

ALTER TABLE WH2016
ADD study_year INTEGER NOT NULL DEFAULT 2016; -- For 2016

ALTER TABLE WH2017
ADD study_year INTEGER NOT NULL DEFAULT 2017; -- For 2017

ALTER TABLE WH2018
ADD study_year INTEGER NOT NULL DEFAULT 2018; -- For 2018

ALTER TABLE WH2019
ADD study_year INTEGER NOT NULL DEFAULT 2019; -- For 2019

-- Determine which countries are missing in some years or spelled differently throughout the studies
-- CTE
WITH all_countries AS (
-- Union Tables for Country Name and Year
	SELECT Country, study_year
	FROM WH2015
	UNION ALL
	SELECT Country, study_year
	FROM WH2016
	UNION ALL
	SELECT Country, study_year
	FROM WH2017
	UNION ALL
	SELECT Country, study_year
	FROM WH2018
	UNION ALL
	SELECT Country, study_year
	FROM WH2019
),

-- Calculate number of years countries are included
total_years AS (
	SELECT Country, COUNT(study_year) as num_years
	FROM all_countries
	GROUP BY Country
)

--Count years for countries
SELECT Country, num_years
FROM total_years
-- filter for less than max
WHERE num_years < 5
-- Order by Country name
ORDER BY Country;

-- Data Cleaning of some Country names
-- ie. (Hong Kong S.A.R., China, Macedonia, Taiwan Province of China, Trinidad & Tobago, North Cyprus)

--Use previous all_countries CTE
WITH all_countries AS (
-- Union Tables for Country Name and Year
	SELECT Country, study_year
	FROM WH2015
	UNION ALL
	SELECT Country, study_year
	FROM WH2016
	UNION ALL
	SELECT Country, study_year
	FROM WH2017
	UNION ALL
	SELECT Country, study_year
	FROM WH2018
	UNION ALL
	SELECT Country, study_year
	FROM WH2019
)

-- Find year with country names that need fixing
SELECT *
FROM all_countries
--filter for country name to edit
WHERE Country IN ('Hong Kong S.A.R., China', 'Macedonia', 'Taiwan Province of China', 'Trinidad & Tobago', 'North Cyprus');

-- Update tables to fix country name discrepancies

-- Set Macedonia to North Macedonia for 2015
UPDATE WH2015
SET Country = 'North Macedonia'
WHERE Country = 'Macedonia';

--Set Macedonia to North Macedonia for 2016
UPDATE WH2016
SET Country = 'North Macedonia'
WHERE Country = 'Macedonia';

--Set Macedonia to North Macedonia for 2017
UPDATE WH2017
SET Country = 'North Macedonia'
WHERE Country = 'Macedonia';

--Set Macedonia to North Macedonia for 2018
UPDATE WH2018
SET Country = 'North Macedonia'
WHERE Country = 'Macedonia';

-- Set Taiwan Province of China to Taiwan for 2017
UPDATE WH2017
SET Country = 'Taiwan'
WHERE Country = 'Taiwan Province of China';

-- Set Hong Kong S.A.R., China to Hong Kong for 2017
UPDATE WH2017
SET Country = 'Hong Kong'
WHERE Country = 'Hong Kong S.A.R., China';

-- Set Trinidad & Tobago to Trinidad and Tobago for 2018
UPDATE WH2018
SET Country = 'Trinidad and Tobago'
WHERE Country = 'Trinidad & Tobago';

--Set Trinidad & Tobago to Trinidad and Tobago for 2019
UPDATE WH2019
SET Country = 'Trinidad and Tobago'
WHERE Country = 'Trinidad & Tobago';

-- Set North Cyprus to Northern Cyprus for 2015
UPDATE WH2015
SET Country = 'Northern Cyprus'
WHERE Country = 'North Cyprus';

--Set North Cyprus to Northern Cyprus for 2016
UPDATE WH2016
SET Country = 'Northern Cyprus'
WHERE Country = 'North Cyprus';

--Set North Cyprus to Northern Cyprus for 2017
UPDATE WH2017
SET Country = 'Northern Cyprus'
WHERE Country = 'North Cyprus';

/* SECTION 3: Joins
Join 2015 Region data with 2017 - 2019 tables while keeping Rank, Country, Happiness Score,
GDP per capita, Family, Healthy life expectancy, Freedom, Generosity, Perceptions of courruption,
and study_year columns. Then combine all tables in the same format.*/

-- Need to convert columns to int/float from VARCHAR

-- CTE with joins above and union all (filter for Null regions)
WITH final2015 AS (
-- SELECT desired columns from 2015 casted to correct data types
	SELECT CAST(Happiness_Rank AS INT) AS Happiness_Rank, Country, Region, CAST(Happiness_Score AS DECIMAL(4,3)) AS Happiness_Score, 
		CAST(GDP_per_capita AS DECIMAL(6,5)) AS GDP_per_capita, CAST(Family AS DECIMAL(6,5)) AS Family, CAST(Healthy_life_expectancy AS DECIMAL(6,5)) AS Healthy_life_expectancy, 
		CAST(Freedom AS DECIMAL(6,5)) AS Freedom, CAST(Generosity AS DECIMAL(6,5)) AS Generosity, CAST(Perceptions_of_corruption AS DECIMAL(6,5)) AS Perceptions_of_corruption, study_year
FROM WH2015),

final2016 AS (
-- SELECT desired columns from 2016
SELECT CAST(Happiness_Rank AS INT) AS Happiness_Rank, Country, Region, CAST(Happiness_Score AS DECIMAL(4,3)) AS Happiness_Score, 
		CAST(GDP_per_capita AS DECIMAL(6,5)) AS GDP_per_capita, CAST(Family AS DECIMAL(6,5)) AS Family, CAST(Healthy_life_expectancy AS DECIMAL(6,5)) AS Healthy_life_expectancy, 
		CAST(Freedom AS DECIMAL(6,5)) AS Freedom, CAST(Generosity AS DECIMAL(6,5)) AS Generosity, CAST(Perceptions_of_corruption AS DECIMAL(6,5)) AS Perceptions_of_corruption, study_year
FROM WH2016),

-- Join Regions to 2017
final2017 AS (
-- Select columns desired in order Happiness Rank, Country, Region,...
SELECT b.Happiness_Rank, b.Country, a.Region, b.Happiness_Score, b.GDP_per_capita, b.Family,
	b.Healthy_life_expectancy, b.Freedom, b.Generosity, b.Perceptions_of_corruption,
	b.study_year
-- FROM WH2015 as a
FROM WH2015 AS a
-- RIGHT JOIN with WH2017 as b to keep only 2017 countries
RIGHT JOIN WH2017 as b
-- Join on Country column
ON a.Country = b.Country),

-- Fix data type of numeric columns for 2018. Had to use TRY_CAST as there was a "N/A" input hiding for UAE
dataFix2018 AS (
-- SELECT desired columns from 2018
SELECT TRY_CAST(Happiness_Rank AS INT) AS Happiness_Rank, Country, TRY_CAST(Happiness_Score AS DECIMAL(4,3)) AS Happiness_Score, 
		TRY_CAST(GDP_per_capita AS DECIMAL(4,3)) AS GDP_per_capita, TRY_CAST(Family AS DECIMAL(4,3)) AS Family, TRY_CAST(Healthy_life_expectancy AS DECIMAL(4,3)) AS Healthy_life_expectancy, 
		TRY_CAST(Freedom AS DECIMAL(4,3)) AS Freedom, TRY_CAST(Generosity AS DECIMAL(4,3)) AS Generosity, TRY_CAST(Perceptions_of_corruption AS DECIMAL(4,3)) AS Perceptions_of_corruption, study_year
FROM WH2018),

-- Join Regions to 2018
final2018 AS (
-- Select columns desired in order Happiness Rank, Country, Region,...
SELECT b.Happiness_Rank, b.Country, a.Region, b.Happiness_Score, b.GDP_per_capita, b.Family,
	b.Healthy_life_expectancy, b.Freedom, b.Generosity, b.Perceptions_of_corruption,
	b.study_year
-- FROM WH2015 as a
FROM WH2015 AS a
-- RIGHT JOIN with WH2018 as b to keep only 2018 countries
RIGHT JOIN dataFix2018 as b
-- Join on Country column
ON a.Country = b.Country),


-- Fix data type of numeric columns for 2019
dataFix2019 AS (
-- SELECT desired columns from 2019
SELECT TRY_CAST(Happiness_Rank AS INT) AS Happiness_Rank, Country, TRY_CAST(Happiness_Score AS DECIMAL(4,3)) AS Happiness_Score, 
		TRY_CAST(GDP_per_capita AS DECIMAL(4,3)) AS GDP_per_capita, TRY_CAST(Family AS DECIMAL(4,3)) AS Family, TRY_CAST(Healthy_life_expectancy AS DECIMAL(4,3)) AS Healthy_life_expectancy, 
		TRY_CAST(Freedom AS DECIMAL(4,3)) AS Freedom, TRY_CAST(Generosity AS DECIMAL(4,3)) AS Generosity, TRY_CAST(Perceptions_of_corruption AS DECIMAL(4,3)) AS Perceptions_of_corruption, study_year
FROM WH2019),

-- Join Regions to 2019
final2019 AS (
-- Select columns desired in order Happiness Rank, Country, Region,...
SELECT b.Happiness_Rank, b.Country, a.Region, b.Happiness_Score, b.GDP_per_capita, b.Family,
	b.Healthy_life_expectancy, b.Freedom, b.Generosity, b.Perceptions_of_corruption,
	b.study_year
-- FROM WH2015 as a
FROM WH2015 AS a
-- RIGHT JOIN with WH2019 as b to keep only 2019 countries
RIGHT JOIN dataFix2019 as b
-- Join on Country column
ON a.Country = b.Country)

SELECT *
FROM final2015
UNION ALL
SELECT *
FROM final2016
UNION ALL
SELECT *
FROM final2017
UNION ALL
SELECT *
FROM final2018
UNION ALL
SELECT *
FROM final2019;