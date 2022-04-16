/*
 * References
 * ----------
 * -  https://calogica.com/sql/2018/12/17/parsing-nested-json-snowflake.html#one-level
 * - reference: https://dwgeek.com/snowflake-convert-array-to-rows-methods-and-examples.html/
 */

--------------------------------create database--------------------------------------------

CREATE DATABASE UDACITY_YELP_CLIMATE;

--------------------------------------------------------------------------------------------

--------------------------------create schemas ---------------------------------------------

CREATE SCHEMA staging;
CREATE SCHEMA ods;
CREATE SCHEMA datawarehouse;

-----------------------------------create formats--------------------------------------------

CREATE OR REPLACE FILE FORMAT myjsonformat
    TYPE = 'JSON' 
    strip_outer_array = TRUE
;

CREATE OR REPLACE FILE FORMAT mycsvformat 
    TYPE = 'CSV' 
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = true
    compression = gzip
;    

---------------------------------------------------------------------------------------------------
------------------------------------------ Staging ------------------------------------------------
---------------------------------------------------------------------------------------------------

USE SCHEMA staging;


CREATE OR REPLACE STAGE my_json_stage file_format = myjsonformat;
CREATE OR REPLACE STAGE my_csv_stage file_format = mycsvformat;

------------------------------------------- Yelp ------------------------------------------------- 

PUT FILE:////Users/ellekawerk/Desktop/udacity/data_architecture_nanodegree/course_2/project/yelp_dataset/yelp_academic_dataset_tip.json 
    @my_json_stage auto_compress=true;

PUT FILE:////Users/ellekawerk/Desktop/udacity/data_architecture_nanodegree/course_2/project/yelp_dataset/yelp_academic_dataset_review.json
    @my_json_stage auto_compress = true;

PUT FILE:////Users/ellekawerk/Desktop/udacity/data_architecture_nanodegree/course_2/project/yelp_dataset/yelp_academic_dataset_user.json 
    @my_json_stage auto_compress=true;

PUT FILE:////Users/ellekawerk/Desktop/udacity/data_architecture_nanodegree/course_2/project/yelp_dataset/yelp_academic_dataset_business.json 
    @my_json_stage auto_compress = true;

PUT FILE:////Users/ellekawerk/Desktop/udacity/data_architecture_nanodegree/course_2/project/yelp_dataset/yelp_academic_dataset_checkin.json 
    @my_json_stage auto_compress = true;

put file:////Users/ellekawerk/Desktop/udacity/data_architecture_nanodegree/course_2/project/yelp_dataset/yelp_academic_dataset_covid_features.json 
    @my_json_stage auto_compress = true;

CREATE TABLE staging.USER(user_json VARIANT);

COPY
INTO
    staging.user
FROM
    @my_json_stage/yelp_academic_dataset_user.json.gz file_format = (format_name = myjsonformat)
on_error = 'skip_file'
;

CREATE TABLE staging.tip(tip_json VARIANT);

COPY
INTO
    staging.tip
FROM
    @my_json_stage/yelp_academic_dataset_tip.json.gz 
file_format = (format_name = myjsonformat)
on_error = 'skip_file';

CREATE TABLE staging.review(review_json VARIANT);

COPY
INTO
    staging.review
FROM
    @my_json_stage/yelp_academic_dataset_review.json.gz 
file_format = (format_name = myjsonformat) 
on_error = 'skip_file'
;

CREATE TABLE checkin(checkin_json VARIANT);

COPY
INTO
    staging.checkin
FROM
    @my_json_stage/yelp_academic_dataset_checkin.json.gz
file_format = (format_name = myjsonformat )
on_error = 'skip_file';

CREATE TABLE staging.business(business_json VARIANT);

COPY
INTO
    staging.business
FROM
    @my_json_stage/yelp_academic_dataset_business.json.gz
file_format = (format_name = myjsonformat) 
on_error = 'skip_file';

CREATE TABLE staging.covid_features(covid_features_json VARIANT);

COPY
INTO 
    staging.covid_features
FROM 
    @my_json_stage/yelp_academic_dataset_covid_features.json.gz
file_format = (format_name = myjsonformat)
on_error = 'skip_file'
;


------------------------------------------- Weather ------------------------------------------------- 

/*
 * Zip Codes to Stations Mapping
 * ------------------------------
 *  https://github.com/bnjmnhndrsn/weather/blob/master/data/zipcodes-normals-stations.txt
 */

PUT 
FILE:////Users/ellekawerk/Desktop/udacity/DA_nanodegree/course_2/project/weather_dataset/USC00356749-PORTLAND_KGW-TV-precipitation-inch.csv 
@my_csv_stage auto_compress = true;

PUT 
FILE:////Users/ellekawerk/Desktop/udacity/DA_nanodegree/course_2/project/weather_dataset/USC00356749-temperature-degreeF.csv
@my_csv_stage auto_compress = true;

LIST @my_csv_stage;

CREATE OR REPLACE TABLE staging.TEMPERATURE(
    date varchar,
    min_fahr varchar,
    max_fahr varchar,
    normal_min_fahr varchar,
    normal_max_fahr varchar
);

COPY INTO 
    staging.TEMPERATURE 
FROM 
    @my_csv_stage/USC00356749-temperature-degreeF.csv.gz
file_format = (format_name = mycsvformat)
on_error = 'skip_file'
;


ALTER TABLE staging.TEMPERATURE 
ADD COLUMN postal_code varchar ;

UPDATE staging.TEMPERATURE 
SET postal_code = '97204'
;

SELECT * FROM staging.TEMPERATURE ;


CREATE OR REPLACE TABLE staging.precipitation(
    date varchar,
    precipitation_inches varchar,
    precipitation_normal_inches varchar
);


COPY INTO 
    staging.PRECIPITATION 
FROM 
    @my_csv_stage/USC00356749-PORTLAND_KGW-TV-precipitation-inch.csv.gz
file_format = (format_name = mycsvformat)
on_error = 'skip_file'
;

ALTER TABLE staging.PRECIPITATION 
ADD COLUMN postal_code varchar ;

UPDATE staging.PRECIPITATION 
SET postal_code = '97204'
;

SELECT * FROM staging.PRECIPITATION ;