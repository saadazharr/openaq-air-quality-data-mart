--- Main Table
CREATE TABLE AirQualityData.dbo.dim_locations (
    id INT PRIMARY KEY IDENTITY(1,1),
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    geo_id INT
);

CREATE TABLE AirQualityData.dbo.dim_geo (
    id INT PRIMARY KEY IDENTITY(1,1),
    country VARCHAR(255),
    city VARCHAR(255)
);

CREATE TABLE AirQualityData.dbo.dim_attribution (
    id INT PRIMARY KEY IDENTITY(1,1),
    name VARCHAR(255)
);

CREATE TABLE AirQualityData.dbo.dim_unit (
    id INT PRIMARY KEY IDENTITY(1,1),
    unit VARCHAR(50)
);

CREATE TABLE AirQualityData.dbo.dim_parameter (
    id INT PRIMARY KEY IDENTITY(1,1),
    parameter VARCHAR(255)
);

CREATE TABLE AirQualityData.dbo.fact_aq (
    id INT PRIMARY KEY IDENTITY(1,1),
    location_id INT,
    geo_id INT,
    utc VARCHAR(55),
    local VARCHAR(55),
    parameter_id INT,
    value FLOAT,
    unit_id INT,
    attribution_id INT, 
    FOREIGN KEY (location_id) REFERENCES dim_locations(id),
    FOREIGN KEY (geo_id) REFERENCES dim_geo(id),
    FOREIGN KEY (parameter_id) REFERENCES dim_parameter(id),
    FOREIGN KEY (unit_id) REFERENCES dim_unit(id),
    FOREIGN KEY (attribution_id) REFERENCES dim_attribution(id) 
);


SELECT * FROM AirQualityData.dbo.[2018-04-04]

INSERT INTO dim_geo (country, city)
SELECT DISTINCT a.country, a.city
FROM AirQualityData.dbo.[all_csv_files] a

INSERT INTO dim_locations (location, latitude, longitude, geo_id)
SELECT DISTINCT a.location, a.latitude, a.longitude, g.id
FROM AirQualityData.dbo.[all_csv_files] a
JOIN dim_geo g 
ON a.country = g.country AND a.city = g.city;

INSERT INTO dim_parameter (parameter)
SELECT DISTINCT a.parameter
FROM AirQualityData.dbo.[all_csv_files] a


INSERT INTO dim_unit (unit)
SELECT DISTINCT a.unit
FROM AirQualityData.dbo.[all_csv_files] a;

select * from dim_unit

select * from AirQualityData.dbo.dim_geo
select * from AirQualityData.dbo.dim_locations
select * from AirQualityData.dbo.dim_parameter
select * from AirQualityData.dbo.dim_unit


SELECT TOP 10 utc, local 
FROM AirQualityData.dbo.[2018-04-04];

INSERT INTO AirQualityData.dbo.fact_aq (location_id, geo_id, utc, local, parameter_id, value, unit_id, attribution_id)
SELECT 
    loc.id AS location_id,
    geo.id AS geo_id,
    src.utc,
    src.local,
    param.id AS parameter_id,
    src.value,
    unit.id AS unit_id,
    attr.id AS attribution_id
FROM AirQualityData.dbo.[all_csv_files] AS src
LEFT JOIN dim_locations loc ON src.location = loc.location AND src.latitude = loc.latitude AND src.longitude = loc.longitude
LEFT JOIN dim_geo geo ON src.country = geo.country AND src.city = geo.city
LEFT JOIN dim_parameter param ON src.parameter = param.parameter
LEFT JOIN dim_unit unit ON src.unit = unit.unit
LEFT JOIN dim_attribution attr ON src.attribution = attr.description; 

select *from dbo.fact_aq

-- Create Staging Schema
CREATE SCHEMA Staging;

-- Create Staging.dim_locations
CREATE TABLE AirQualityData.Staging.dim_locations (
    id INT PRIMARY KEY IDENTITY(1,1),
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    geo_id INT
);

-- Create Staging.dim_geo
CREATE TABLE AirQualityData.Staging.dim_geo (
    id INT PRIMARY KEY IDENTITY(1,1),
    country VARCHAR(255),
    city VARCHAR(255)
);

-- Create Staging.dim_parameter
CREATE TABLE AirQualityData.Staging.dim_parameter (
    id INT PRIMARY KEY IDENTITY(1,1),
    parameter VARCHAR(255)
);

-- Create Staging.dim_unit
CREATE TABLE AirQualityData.Staging.dim_unit (
    id INT PRIMARY KEY IDENTITY(1,1),
    unit VARCHAR(50)
);

-- Create Staging.dim_attribution
CREATE TABLE AirQualityData.Staging.dim_attribution (
    id INT PRIMARY KEY IDENTITY(1,1),
    name VARCHAR(1000)
);

-- Create Staging.fact_aq
CREATE TABLE AirQualityData.Staging.fact_aq (
    id INT PRIMARY KEY IDENTITY(1,1),
    location_id INT,
    geo_id INT,
    utc VARCHAR(55),
    local VARCHAR(55),
    parameter_id INT,
    value FLOAT,
    unit_id INT,
    attribution_id INT,
    FOREIGN KEY (location_id) REFERENCES AirQualityData.Staging.dim_locations(id),
    FOREIGN KEY (geo_id) REFERENCES AirQualityData.Staging.dim_geo(id),
    FOREIGN KEY (parameter_id) REFERENCES AirQualityData.Staging.dim_parameter(id),
    FOREIGN KEY (unit_id) REFERENCES AirQualityData.Staging.dim_unit(id),
    FOREIGN KEY (attribution_id) REFERENCES AirQualityData.Staging.dim_attribution(id)
);

-- Create Production Schema
CREATE SCHEMA Production;

-- Create Production.dim_locations
CREATE TABLE AirQualityData.Production.dim_locations (
    id INT PRIMARY KEY,
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT,
    geo_id INT
);

-- Create Production.dim_geo
CREATE TABLE AirQualityData.Production.dim_geo (
    id INT PRIMARY KEY,
    country VARCHAR(255),
    city VARCHAR(255)
);

-- Create Production.dim_parameter
CREATE TABLE AirQualityData.Production.dim_parameter (
    id INT PRIMARY KEY,
    parameter VARCHAR(255)
);

-- Create Production.dim_unit
CREATE TABLE AirQualityData.Production.dim_unit (
    id INT PRIMARY KEY,
    unit VARCHAR(50)
);

-- Create Production.dim_attribution
CREATE TABLE AirQualityData.Production.dim_attribution (
    id INT PRIMARY KEY,
    name VARCHAR(1000)
);

-- Create Production.fact_aq
CREATE TABLE AirQualityData.Production.fact_aq (
    id INT PRIMARY KEY IDENTITY(1,1),
    location_id INT,
    geo_id INT,
    utc VARCHAR(55),
    local VARCHAR(55),
    parameter_id INT,
    value FLOAT,
    unit_id INT,
    attribution_id INT,
    FOREIGN KEY (location_id) REFERENCES AirQualityData.Production.dim_locations(id),
    FOREIGN KEY (geo_id) REFERENCES AirQualityData.Production.dim_geo(id),
    FOREIGN KEY (parameter_id) REFERENCES AirQualityData.Production.dim_parameter(id),
    FOREIGN KEY (unit_id) REFERENCES AirQualityData.Production.dim_unit(id),
    FOREIGN KEY (attribution_id) REFERENCES AirQualityData.Production.dim_attribution(id)
);

-- Populating the Staging.

-- Populate dim_geo
INSERT INTO AirQualityData.Staging.dim_geo (country, city)
SELECT DISTINCT a.country, a.city
FROM AirQualityData.dbo.[2018-04-04] a;

-- Populate dim_locations
INSERT INTO AirQualityData.Staging.dim_locations (location, latitude, longitude, geo_id)
SELECT DISTINCT a.location, a.latitude, a.longitude, g.id
FROM AirQualityData.dbo.[2018-04-04] a
JOIN AirQualityData.Staging.dim_geo g 
ON a.country = g.country AND a.city = g.city;

INSERT INTO AirQualityData.Staging.dim_parameter (parameter)
SELECT DISTINCT a.parameter
FROM AirQualityData.dbo.[2018-04-04] a;

INSERT INTO AirQualityData.Staging.dim_unit (unit)
SELECT DISTINCT a.unit
FROM AirQualityData.dbo.[2018-04-04] a;

INSERT INTO AirQualityData.Staging.dim_attribution (name)
SELECT DISTINCT a.attribution
FROM AirQualityData.dbo.[2018-04-04] a;

INSERT INTO AirQualityData.Staging.fact_aq (location_id, geo_id, utc, local, parameter_id, value, unit_id, attribution_id)
SELECT 
    loc.id AS location_id,
    geo.id AS geo_id,
    src.utc,
    src.local,
    param.id AS parameter_id,
    src.value,
    unit.id AS unit_id,
    attr.id AS attribution_id
FROM AirQualityData.dbo.[2018-04-04] AS src
LEFT JOIN AirQualityData.Staging.dim_locations loc 
    ON src.location = loc.location 
    AND src.latitude = loc.latitude 
    AND src.longitude = loc.longitude
LEFT JOIN AirQualityData.Staging.dim_geo geo 
    ON src.country = geo.country 
    AND src.city = geo.city
LEFT JOIN AirQualityData.Staging.dim_parameter param 
    ON src.parameter = param.parameter
LEFT JOIN AirQualityData.Staging.dim_unit unit 
    ON src.unit = unit.unit
LEFT JOIN AirQualityData.Staging.dim_attribution attr 
    ON src.attribution = attr.name;

-- Populate Now Production_layer

INSERT INTO AirQualityData.Production.dim_geo (id, country, city)
SELECT id, country, city
FROM AirQualityData.Staging.dim_geo;

INSERT INTO AirQualityData.Production.dim_locations (id, location, latitude, longitude, geo_id)
SELECT id, location, latitude, longitude, geo_id
FROM AirQualityData.Staging.dim_locations;

INSERT INTO AirQualityData.Production.dim_parameter (id, parameter)
SELECT id, parameter
FROM AirQualityData.Staging.dim_parameter;

INSERT INTO AirQualityData.Production.dim_unit (id, unit)
SELECT id, unit
FROM AirQualityData.Staging.dim_unit;

INSERT INTO AirQualityData.Production.dim_attribution (id, name)
SELECT id, name
FROM AirQualityData.Staging.dim_attribution;

INSERT INTO AirQualityData.Production.fact_aq (
    location_id, geo_id, utc, local, parameter_id, value, unit_id, attribution_id
)
SELECT 
    location_id, 
    geo_id, 
    utc, 
    local, 
    parameter_id, 
    value, 
    unit_id, 
    attribution_id
FROM AirQualityData.Staging.fact_aq;

SELECT 
    (SELECT COUNT(*) FROM AirQualityData.Staging.dim_geo) AS Staging_RowCount,
    (SELECT COUNT(*) FROM AirQualityData.Production.dim_geo) AS Production_RowCount;

DELETE FROM AirQualityData.Staging.fact_aq;
DELETE FROM AirQualityData.Staging.dim_locations;
DELETE FROM AirQualityData.Staging.dim_geo;
DELETE FROM AirQualityData.Staging.dim_parameter;
DELETE FROM AirQualityData.Staging.dim_unit;
DELETE FROM AirQualityData.Staging.dim_attribution;


--- Now Catering New File

INSERT INTO AirQualityData.Staging.dim_geo (country, city)
SELECT DISTINCT a.country, a.city
FROM AirQualityData.dbo.[2018-04-03] a;


INSERT INTO AirQualityData.Staging.dim_locations (location, latitude, longitude, geo_id)
SELECT DISTINCT a.location, a.latitude, a.longitude, g.id
FROM AirQualityData.dbo.[2018-04-03] a
JOIN AirQualityData.Staging.dim_geo g 
ON a.country = g.country AND a.city = g.city;

INSERT INTO AirQualityData.Staging.dim_parameter (parameter)
SELECT DISTINCT a.parameter
FROM AirQualityData.dbo.[2018-04-03] a;

INSERT INTO AirQualityData.Staging.dim_unit (unit)
SELECT DISTINCT a.unit
FROM AirQualityData.dbo.[2018-04-03] a;

INSERT INTO AirQualityData.Staging.dim_attribution (name)
SELECT DISTINCT a.attribution
FROM AirQualityData.dbo.[2018-04-03] a;

ALTER TABLE AirQualityData.staging.dim_attribution
ALTER COLUMN name VARCHAR(5000);

INSERT INTO AirQualityData.Staging.fact_aq (location_id, geo_id, utc, local, parameter_id, value, unit_id, attribution_id)
SELECT 
    loc.id AS location_id,
    geo.id AS geo_id,
    src.utc,
    src.local,
    param.id AS parameter_id,
    src.value,
    unit.id AS unit_id,
    attr.id AS attribution_id
FROM AirQualityData.dbo.[2018-04-03] AS src
LEFT JOIN AirQualityData.Staging.dim_locations loc 
    ON src.location = loc.location 
    AND src.latitude = loc.latitude 
    AND src.longitude = loc.longitude
LEFT JOIN AirQualityData.Staging.dim_geo geo 
    ON src.country = geo.country 
    AND src.city = geo.city
LEFT JOIN AirQualityData.Staging.dim_parameter param 
    ON src.parameter = param.parameter
LEFT JOIN AirQualityData.Staging.dim_unit unit 
    ON src.unit = unit.unit
LEFT JOIN AirQualityData.Staging.dim_attribution attr 
    ON src.attribution = attr.name;

-- Now Updating the Production Layer as per Requirement

MERGE AirQualityData.Production.dim_geo AS target
USING (SELECT id, country, city FROM AirQualityData.Staging.dim_geo) AS source
ON target.id = source.id
WHEN MATCHED THEN 
    UPDATE SET target.country = source.country, target.city = source.city
WHEN NOT MATCHED THEN
    INSERT (id, country, city) VALUES (source.id, source.country, source.city);

Select * from AirQualityData.Production.dim_geo

MERGE AirQualityData.Production.dim_locations AS target
USING (SELECT id, location, latitude, longitude, geo_id FROM AirQualityData.Staging.dim_locations) AS source
ON target.id = source.id
WHEN MATCHED THEN 
    UPDATE SET target.location = source.location, target.latitude = source.latitude, target.longitude = source.longitude, target.geo_id = source.geo_id
WHEN NOT MATCHED THEN
    INSERT (id, location, latitude, longitude, geo_id) VALUES (source.id, source.location, source.latitude, source.longitude, source.geo_id);

Select * from AirQualityData.Production.dim_locations

MERGE AirQualityData.Production.dim_parameter AS target
USING (SELECT id, parameter FROM AirQualityData.Staging.dim_parameter) AS source
ON target.id = source.id
WHEN MATCHED THEN 
    UPDATE SET target.parameter = source.parameter
WHEN NOT MATCHED THEN
    INSERT (id, parameter) VALUES (source.id, source.parameter);

Select * from AirQualityData.Production.dim_parameter

MERGE AirQualityData.Production.dim_unit AS target
USING (SELECT id, unit FROM AirQualityData.Staging.dim_unit) AS source
ON target.id = source.id
WHEN MATCHED THEN 
    UPDATE SET target.unit = source.unit
WHEN NOT MATCHED THEN
    INSERT (id, unit) VALUES (source.id, source.unit);

Select * from AirQualityData.Production.dim_unit

MERGE AirQualityData.Production.dim_attribution AS target
USING (SELECT id, name FROM AirQualityData.Staging.dim_attribution) AS source
ON target.id = source.id
WHEN MATCHED THEN 
    UPDATE SET target.name = source.name
WHEN NOT MATCHED THEN
    INSERT (id, name) VALUES (source.id, source.name);

Select * from AirQualityData.Production.dim_attribution

-- Update and Insert Production.fact_aq 
MERGE AirQualityData.Production.fact_aq AS target
USING (SELECT location_id, geo_id, utc, local, parameter_id, value, unit_id, attribution_id FROM AirQualityData.Staging.fact_aq) AS source
ON target.location_id = source.location_id
WHEN MATCHED THEN 
    UPDATE SET target.geo_id = source.geo_id, target.utc = source.utc, target.local = source.local, 
               target.parameter_id = source.parameter_id, target.value = source.value, 
               target.unit_id = source.unit_id, target.attribution_id = source.attribution_id
WHEN NOT MATCHED THEN
    INSERT (location_id, geo_id, utc, local, parameter_id, value, unit_id, attribution_id)
    VALUES (source.location_id, source.geo_id, source.utc, source.local, source.parameter_id, source.value, source.unit_id, source.attribution_id);

Select * from AirQualityData.Production.dim_locations

Use AirQualityData

CREATE TABLE dbo.all_csv_files (
    location VARCHAR(1000),
    city VARCHAR(1000),
    country VARCHAR(1000),
    utc VARCHAR(1000),        
    local VARCHAR(1000),       
    parameter VARCHAR(1000),
    value FLOAT,             
    unit VARCHAR(1000),
    latitude FLOAT,          
    longitude FLOAT,         
    attribution VARCHAR(max)
);

Use AirQualityData

Select * from dbo.all_csv_files
Drop Table dbo.all_csv_files
delete from dbo.all_csv_files

select * from production.dim_parameter





ALTER TABLE dbo.[2018-04-05]
ALTER COLUMN location VARCHAR(MAX);


select top 10 * from Production.fact_aq