DROP SCHEMA IF EXISTS "water" CASCADE;
CREATE SCHEMA "water";
SET search_path TO "water";

--------------------
---TABLE CREATION---
--------------------


CREATE TABLE "COUNTRIES" (
		"country"						TEXT		NOT NULL,
		"region"						TEXT		NOT NULL,
		"countryId"						INTEGER		PRIMARY KEY
);

CREATE TABLE "YEARS" ( 
		"year"							INTEGER		NOT NULL,		
		"yearId"						INTEGER		PRIMARY KEY
);

CREATE TABLE "MASTER" (
		"idNumber"						INTEGER		PRIMARY KEY,
		"countryId"						INTEGER		NOT NULL,
		"yearId"						INTEGER		NOT NULL,
		FOREIGN KEY ("countryId") REFERENCES "COUNTRIES"("countryId"),
		FOREIGN KEY ("yearId") REFERENCES "YEARS"("yearId")
);

CREATE TABLE "DISEASE_STATS" (
		"diseaseId"						INTEGER		NOT NULL,
		"diarrhealCasesPer100k"			INTEGER		NOT NULL,
		"choleraCasesPer100k"			INTEGER		NOT NULL,
		"typhoidCasesPer100k"			INTEGER		NOT NULL,
		"infantMortalityRatePer1000"	FLOAT		NOT NULL,
		FOREIGN KEY ("diseaseId") REFERENCES "MASTER"("idNumber")

);

CREATE TABLE "WATER_QUALITY" (
		"qualityId"						INTEGER		NOT NULL,
		"waterSourceType"				TEXT		NOT NULL,
		"contaminantLevelPpm"			FLOAT		NOT NULL,
		"pHLevel"						FLOAT		NOT NULL,
		"turbidityNtu"					FLOAT		NOT NULL,
		"dissolvedOxygenMgL"			FLOAT		NOT NULL,
		"nitrateLevelMgL"				FLOAT		NOT NULL,
		"leadConcentrationUgL"			FLOAT		NOT NULL,
		"bacteriaCountCfuMl"			FLOAT		NOT NULL,
		"waterTreatmentMethod"			TEXT		NOT NULL,
		FOREIGN KEY ("qualityId") REFERENCES "MASTER"("idNumber")

);

CREATE TABLE "INFRASTRUCTURE_POLICY" (
		"infrastructureId"				INTEGER		NOT NULL,
		"waterRegulationScore"			FLOAT		NOT NULL,
		"numberOfWaterFacilities"		INTEGER		NOT NULL,
		"governmentSpendingOnWaterUsd"	MONEY		NOT NULL,
		"policyType"					TEXT		NOT NULL,
		"waterEducationProgram"			BOOLEAN		NOT NULL,
		FOREIGN KEY ("infrastructureId") REFERENCES "MASTER"("idNumber"),
		CHECK ("waterRegulationScore" BETWEEN 0 AND 10)
);

CREATE TABLE "SOCIOECONOMIC" (
		"socioeconomicId"				INTEGER		NOT NULL,
		"gdpPerCapitaUsd"				INTEGER		NOT NULL,
		"healthcareAccessIndex"			FLOAT		NOT NULL,
		"urbanizationRatePercent"		FLOAT		NOT NULL,
		"sanitationCoveragePercent"		FLOAT		NOT NULL,
		"populationDensityPerKm2"		INTEGER		NOT NULL,
		"accessToCleanWaterPercent" 	FLOAT 		NOT NULL,
		FOREIGN KEY ("socioeconomicId") REFERENCES "MASTER"("idNumber"),
		CHECK ("healthcareAccessIndex" BETWEEN 0 AND 100)
);

-----------------
---DATA INSERT---
-----------------


\copy "COUNTRIES" FROM COUNTRIES.csv CSV HEADER
\copy "YEARS" FROM YEARS.csv CSV HEADER
\copy "MASTER" FROM MASTER.csv CSV HEADER
\copy "DISEASE_STATS" FROM DISEASE_STATS.csv CSV HEADER
\copy "WATER_QUALITY" FROM WATER_QUALITY.csv CSV HEADER
\copy "INFRASTRUCTURE_POLICY" FROM INFRASTRUCTURE_POLICY.csv CSV HEADER
\copy "SOCIOECONOMIC" FROM SOCIOECONOMIC.csv CSV HEADER

-------------
---QUERIES---
-------------


-- Government official finding the average spending on water within his country
SELECT AVG("governmentSpendingOnWaterUsd"::NUMERIC) AS "Average Spending", "country" AS "Country"
FROM "INFRASTRUCTURE_POLICY"
INNER JOIN "MASTER" 
ON "INFRASTRUCTURE_POLICY"."infrastructureId" = "MASTER"."idNumber"
INNER JOIN "COUNTRIES"
ON "MASTER"."countryId" = "COUNTRIES"."countryId"
GROUP BY "country" 
HAVING "country" = 'USA'
ORDER BY "Average Spending" DESC
;

-- Query for an Environmental Researcher to report on the countries with the highest average contaminant level (PPM)
SELECT  "country" AS "Country", AVG("contaminantLevelPpm"::FLOAT(9)) AS "Contaminant Level (Parts per Million)"
FROM "WATER_QUALITY"
INNER JOIN "MASTER"
ON "WATER_QUALITY"."qualityId" = "MASTER"."idNumber"
INNER JOIN "COUNTRIES"
ON "MASTER"."idNumber" = "COUNTRIES"."countryId"
GROUP BY "Country"
ORDER BY "Contaminant Level (Parts per Million)" DESC
LIMIT 15
;
 
-- Query for a government official to find statistics on infant mortality rate cuased by the water
CREATE OR REPLACE FUNCTION infant_mortality_fn(country_name TEXT)
RETURNS TABLE (
    country TEXT,
    infantMortalityRatePer1000 INTEGER
)
LANGUAGE plpgsql
	AS $$
	BEGIN
		RETURN QUERY
		SELECT "COUNTRIES"."country", AVG(CEIL("DISEASE_STATS"."infantMortalityRatePer1000"))::INTEGER
		FROM "DISEASE_STATS"
		RIGHT OUTER JOIN "MASTER" 
			ON "MASTER"."idNumber" = "DISEASE_STATS"."diseaseId" 
		INNER JOIN "COUNTRIES" 
			ON "MASTER"."countryId" = "COUNTRIES"."countryId"
		WHERE "COUNTRIES"."country" = country_name
		GROUP BY "COUNTRIES"."country";
	END;
	$$ ;
--EXAMPLE QUERY
--SELECT * FROM infant_mortality_fn('Mexico');

-- Query for a health researcher to search previous records for countries HAI
CREATE OR REPLACE FUNCTION healthcare_access_yearly_fn(year_number INTEGER)
RETURNS TABLE (
	"year" 					INTEGER,
    "country" 				TEXT,
	"region"				TEXT,
    "healthcareAccessIndex" DOUBLE PRECISION
)
LANGUAGE plpgsql
	AS $$
	BEGIN
	RETURN QUERY
		SELECT "YEARS"."year", "COUNTRIES"."country", "COUNTRIES"."region", "SOCIOECONOMIC"."healthcareAccessIndex"
		FROM "SOCIOECONOMIC"
		RIGHT OUTER JOIN "MASTER" 
			ON "MASTER"."idNumber" = "SOCIOECONOMIC"."socioeconomicId"
		INNER JOIN "COUNTRIES" 
			ON "MASTER"."countryId" = "COUNTRIES"."countryId"
		INNER JOIN "YEARS" 
			ON "MASTER"."yearId" = "YEARS"."yearId"
		WHERE "YEARS"."year" = year_number
		ORDER BY "healthcareAccessIndex" DESC;
	END;
	$$;

--SELECT * FROM healthcare_access_yearly_fn('2020');

--Query for a researcher to study countries by a minimum urbanization rate.
CREATE OR REPLACE FUNCTION min_urbanization_fn(min_urbanization_rate FLOAT)
RETURNS TABLE (
	"year"				INTEGER,
	"country"			TEXT,
	"urbanizationRate"	FLOAT
)
LANGUAGE plpgsql
	AS $$
	BEGIN
	RETURN QUERY
		SELECT "YEARS"."year", "COUNTRIES"."country", "SOCIOECONOMIC"."urbanizationRatePercent"
		FROM "SOCIOECONOMIC"
		INNER JOIN "MASTER"
			ON "SOCIOECONOMIC"."socioeconomicId" = "MASTER"."idNumber"
		INNER JOIN "COUNTRIES" 
			ON "MASTER"."countryId" = "COUNTRIES"."countryId"
		INNER JOIN "YEARS" 
			ON "MASTER"."yearId" = "YEARS"."yearId"
		WHERE "SOCIOECONOMIC"."urbanizationRatePercent" >= min_urbanization_rate
		ORDER BY "urbanizationRatePercent" DESC
		LIMIT 30;
	END;
	$$;
	
--SELECT * FROM min_urbanization_fn('75');

--Query for a public health worker to find which area of a certain country needs the most help
CREATE OR REPLACE FUNCTION country_help_fn(year_number INTEGER, country_name TEXT, san_coverage FLOAT)
RETURNS TABLE (
	"year"						INTEGER,
	"region"					TEXT,
	"country"					TEXT,
	"sanitationCoveragePercent"	FLOAT
)
LANGUAGE plpgsql
	AS $$
	BEGIN
	RETURN QUERY
		SELECT "YEARS"."year", "COUNTRIES"."region", "COUNTRIES"."country", "SOCIOECONOMIC"."sanitationCoveragePercent"
		FROM "SOCIOECONOMIC"
		INNER JOIN "MASTER" 
			ON "MASTER"."idNumber" = "SOCIOECONOMIC"."socioeconomicId"
		INNER JOIN "COUNTRIES" 
			ON "MASTER"."countryId" = "COUNTRIES"."countryId"
		INNER JOIN "YEARS" 
			ON "MASTER"."yearId" = "YEARS"."yearId"
		WHERE "YEARS"."year" = year_number
		AND "COUNTRIES"."country" = country_name
		AND "SOCIOECONOMIC"."sanitationCoveragePercent" <= san_coverage
		ORDER BY "sanitationCoveragePercent" ASC;
	END;
	$$;
 
 --SELECT * FROM country_help_fn('2020', 'Bangladesh', '50');

--Query for government official to find areas with the lowest healthcare access per nation

CREATE OR REPLACE FUNCTION low_healthcare_fn(min_healthcare FLOAT)
RETURNS TABLE (
    "country" 				TEXT,
    "healthcareAccessIndex"	FLOAT
)
LANGUAGE plpgsql
	AS $$
	BEGIN
		RETURN QUERY
		SELECT "COUNTRIES"."country", "SOCIOECONOMIC"."healthcareAccessIndex"
		FROM "SOCIOECONOMIC"
		INNER JOIN "MASTER" 
			ON "MASTER"."idNumber" = "SOCIOECONOMIC"."socioeconomicId"
		INNER JOIN "COUNTRIES" 
			ON "MASTER"."countryId" = "COUNTRIES"."countryId"
		WHERE "SOCIOECONOMIC"."healthcareAccessIndex" <= min_healthcare
		ORDER BY "healthcareAccessIndex" ASC
		LIMIT 15;
	END;
	$$;

--SELECT * FROM low_healthcare_fn('70');

--Query for a health volunteer to find countries with low clean water access

SELECT "COUNTRIES"."country", "SOCIOECONOMIC"."accessToCleanWaterPercent"
FROM "SOCIOECONOMIC"
INNER JOIN "MASTER" 
    ON "MASTER"."idNumber" = "SOCIOECONOMIC"."socioeconomicId"
INNER JOIN "COUNTRIES" 
    ON "MASTER"."countryId" = "COUNTRIES"."countryId"
WHERE "SOCIOECONOMIC"."accessToCleanWaterPercent" < 70
ORDER BY "accessToCleanWaterPercent" ASC
LIMIT 10;

-- Query for clean water volunteers to find out who spends the least on water for a comprehensive report

SELECT "COUNTRIES"."country", "INFRASTRUCTURE_POLICY"."governmentSpendingOnWaterUsd"
FROM "INFRASTRUCTURE_POLICY"
INNER JOIN "MASTER" 
    ON "MASTER"."idNumber" = "INFRASTRUCTURE_POLICY"."infrastructureId"
INNER JOIN "COUNTRIES" 
    ON "MASTER"."countryId" = "COUNTRIES"."countryId"
WHERE "INFRASTRUCTURE_POLICY"."governmentSpendingOnWaterUsd"::NUMERIC < 1000000
ORDER BY "governmentSpendingOnWaterUsd"::NUMERIC DESC
LIMIT 10
;




-----------------------
---TRIGGER FUNCTIONS---
-----------------------

CREATE FUNCTION incomplete_data_fn()
RETURNS TRIGGER
LANGUAGE plpgsql 
	AS $$ 
		BEGIN 
		-- Applying ELSIF: A conditional statement that  continues an IF statement
		IF NEW."gdpPerCapitaUsd" IS NULL THEN
		RAISE EXCEPTION 'ERROR - Country GDP cannot be NULL.';
		ELSIF NEW."healthcareAccessIndex" IS NULL THEN
		RAISE EXCEPTION 'ERROR - Healthcare Access Index cannot be NULL.';
		ELSIF NEW."urbanizationRatePercent" IS NULL THEN
		RAISE EXCEPTION 'ERROR - Urbanization Rate cannot be NULL.';
		ELSIF NEW."sanitationCoveragePercent" IS NULL THEN
		RAISE EXCEPTION 'ERROR - Sanitation Coverage cannot be NULL.';
		ELSIF NEW."populationDensityPerKm2" IS NULL THEN
		RAISE EXCEPTION 'ERROR - Population Density cannot be NULL.';
		END IF;
		RETURN NEW;
	END;
	$$ 
;
	
CREATE TRIGGER incomplete_data_tr
BEFORE INSERT OR UPDATE ON "SOCIOECONOMIC"
FOR EACH ROW 
EXECUTE FUNCTION incomplete_data_fn()
;

-- TRIGGER function to alert when water bacteria levels are high
CREATE FUNCTION high_bacteria_fn()
RETURNS TRIGGER 
LANGUAGE plpgsql
	AS $$
		BEGIN
		IF NEW."bacteriaCountCfuMl" >= 5000 THEN 
		RAISE EXCEPTION 'WARNING - Bacteria Count is Dangerously High';
		END IF;
		RETURN NEW;
	END;
	$$;


CREATE TRIGGER high_bacteria_tr
BEFORE INSERT OR UPDATE ON "WATER_QUALITY"
FOR EACH ROW
EXECUTE FUNCTION high_bacteria_fn()
;
