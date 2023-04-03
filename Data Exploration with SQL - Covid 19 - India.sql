/*
    Data Exploration: Covid-19: India
    
*/

SELECT *
FROM covid_death_world;

SELECT *
FROM covid_vaccination_world;

-- Selecting Data related to India

SELECT *
FROM covid_death_world
WHERE LOCATION = 'India'
ORDER BY LOCATION;

SELECT *
FROM covid_vaccination_world
WHERE LOCATION = 'India'
ORDER BY date;

--Creating a temp table: covid_vaccination_india and covid_death_india

--covid_death_india

CREATE TEMP TABLE covid_death_india (LIKE covid_death_world);

INSERT INTO covid_death_india (
    SELECT *
    FROM covid_death_world
    WHERE LOCATION = 'India'
);

SELECT *
FROM covid_death_india;

--covid_vaccination_india

CREATE TEMP TABLE covid_vaccination_india (LIKE covid_vaccination_world);

INSERT INTO covid_vaccination_india (
    SELECT *
    FROM covid_vaccination_world
    WHERE LOCATION = 'India'
);

SELECT *
FROM covid_vaccination_india;

-- First Case and First Death

SELECT *
FROM covid_death_india
ORDER BY date;

--First Case Reported on: 

SELECT min(date) AS "First_Case_Reported_On"
FROM covid_death_india
WHERE total_cases = 1;

--First Death Reported on: 

SELECT min(date) AS "First_Death_Reported_On"
FROM covid_death_india
WHERE total_deaths = 1;

--The highest number of new cases recorded on:

SELECT date AS "Date", 
    new_cases AS "Number_of_new_cases_reported"
FROM covid_death_india
WHERE new_cases = (
    SELECT max(new_cases)
    FROM covid_death_india
);

-- Case to death ratio: total_deaths / total_new_cases:

SELECT total_deaths, 
    total_cases, 
    total_deaths::float4 / total_cases::float4 AS "case_to_death"
FROM covid_death_india;

-- January 2020 - March 2023

-- Stored Function

CREATE OR REPLACE FUNCTION death_rate_monthly_year(years INT) RETURNS TABLE (months VARCHAR, deaths BIGINT, death_rate float4) LANGUAGE SQL AS 
$$
select case 
    when extract(month from date) = 1 
        then 'January'
    when extract(month from date) = 2
        then 'February'
    when extract(month from date) = 3 
        then 'March'
    when extract(month from date) = 4 
        then 'April'
    when extract(month from date) = 5 
        then 'May'
    when extract(month from date) = 6 
        then 'June'
    when extract(month from date) = 7 
        then 'July'
    when extract(month from date) = 8
        then 'August'
    when extract(month from date) = 9
        then 'September'
    when extract(month from date) = 10
        then 'October'
    WHEN extract(month from date) = 11
        then 'November'
    when extract(month from date) = 12
     then 'December'
    end as "Months",
sum(new_deaths) as "deaths", sum(new_deaths)::float4 / population::float4 as "death_rate"
from covid_death_india
where extract(year from date) = years
group by extract(month from date),population
order by extract(month from date)

$$;
-- Death Rate - Monthly interval -2020 - 2023

SELECT * 
FROM death_rate_monthly_year(2020);
SELECT * 
FROM death_rate_monthly_year(2021);
SELECT * 
FROM death_rate_monthly_year(2022);
SELECT * 
FROM death_rate_monthly_year(2023);


-- Death Rate - Yearly Interval

SELECT 
    EXTRACT(YEAR FROM date) AS YEAR, 
    sum(new_deaths) AS deaths,  
    sum(new_deaths)::float4 / population::float4 AS "death_rate" 
FROM covid_death_india
GROUP BY EXTRACT(YEAR FROM date),population;

SELECT 
    max(total_deaths) AS total_deaths, 
    max(total_deaths)::float4 / population::float4 AS "death_rate" 
FROM covid_death_india
GROUP BY population;

-- total cases vs population

SELECT max(total_cases) AS "total_cases", 
    population, max(total_cases)::float4 / population::float4 AS "cases_vs_population_ratio"
FROM covid_death_india
GROUP BY population ;

-- total_cases delta from previous day
SELECT date, 
    total_cases, 
    total_cases - lag(total_cases,1) OVER(
        ORDER BY date
    ) AS total_cases_delta
FROM covid_death_india;

--total_death delta from previous day

SELECT date, 
    total_deaths, 
    total_deaths - lag(total_deaths,1) OVER(
        ORDER BY date
    ) AS total_death_delta
FROM covid_death_india
WHERE total_deaths IS NOT NULL;

-- total_vaccination vs total_cases

SELECT date, 
    total_cases, 
    total_vaccinations, 
    total_vaccinations::float4 / total_cases::float4 * 100 AS "_percent_total_cases_vs_vaccinations"
FROM covid_vaccination_india 
JOIN covid_death_india USING(date)
WHERE total_vaccinations IS NOT NULL;

-- percentage of population vaccinated - partially

SELECT max(people_vaccinated) AS "people_vaccinated", 
    population, 
    max(people_vaccinated)::float4 / population::float4 * 100 AS "percent_people_vaccinated_vs_population"
FROM covid_vaccination_india
GROUP BY population;

-- percentage of population fully vaccinated - first + second dose

SELECT max(people_fully_vaccinated) AS "people_fully_vaccinated", 
    population, 
    max(people_fully_vaccinated)::float4 / population::float4 * 100 AS "percent_people_fully_vaccinated_vs_population"
FROM covid_vaccination_india
GROUP BY population;

-- percentage of population who have taken booster dose 

SELECT max(total_boosters) AS "total_booster_taken", 
    population, 
    max(total_boosters)::float4 / population * 100 AS "percent_booster_taken"
FROM covid_vaccination_india;

-- total vaccination drive (partial vaccination + fully vaccinated + booster ) 

SELECT  max(total_vaccinations) AS "total_vaccination_drive",
    population
FROM covid_vaccination_india
GROUP BY population;
  
--ratio of total_cases, total_deaths in india to that of asia
CREATE OR REPLACE VIEW asia_case_deaths AS (
    SELECT LOCATION,
        max(total_cases) AS "max_cases_location", 
        max(total_deaths) AS "max_deaths_location"
    FROM covid_death_world
    WHERE continent = 'Asia'
    GROUP BY LOCATION
)

--total_cases in india vs total_cases in world

SELECT 
    (SELECT max(total_cases)FROM covid_death_india) AS "total_cases_india", 
    sum(max_cases_location) AS total_cases_asia, 
    (SELECT max(total_cases)::float4 FROM covid_death_india) / sum(max_cases_location)::float4 * 100 AS "total_cases_india_to_world" 
FROM asia_case_deaths;

-- total_deaths in india vs total_deaths in world
SELECT 
    (SELECT max(total_deaths)FROM covid_death_india) AS "total_deaths_india", 
    sum(max_deaths_location) AS total_deaths_asia, 
    (SELECT max(total_deaths)::float4 FROM covid_death_india) / sum(max_deaths_location)::float4 * 100 AS "total_deaths_india_to_world" 
FROM asia_case_deaths;

--ratio of total_cases, total_deaths in india to that of world

CREATE OR REPLACE VIEW view_world AS (
    SELECT max(total_cases) AS "total_cases_world", 
        max(total_deaths) AS "total_deaths_world"
    FROM covid_death_world 
    WHERE LOCATION = 'World'
)

-- total cases in india vs total_cases wold percent
SELECT 
    (SELECT max(total_cases) FROM covid_death_india) AS "total_cases_india", 
    total_cases_world, 
    (SELECT max(total_cases) FROM covid_death_india)::float4 / total_cases_world::float4 *100 AS "percent_cases_india_to_world"
FROM view_world

--total deaths in india vs total_cases_world percent
SELECT 
    (SELECT max(total_deaths) FROM covid_death_india) AS "total_deaths_india", 
    total_deaths_world, 
    (SELECT max(total_deaths) FROM covid_death_india)::float4 / total_deaths_world::float4 *100 AS "percent_death_india_to_world"
FROM view_world