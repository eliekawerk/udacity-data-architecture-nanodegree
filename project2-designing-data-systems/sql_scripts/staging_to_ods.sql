-------------------------------------------------------------------------------------------------------
---------------------------------------------------- ODS ----------------------------------------------
-------------------------------------------------------------------------------------------------------

------------------------------------------------ Yelp ------------------------------------------------- 


------------------------------------------------- Checkin ---------------------------------------------

CREATE OR REPLACE TABLE ods.checkin(
    checkin_id integer AUTOINCREMENT NOT NULL,
    business_id varchar,
    checkin_timestamp timestamp,
    CONSTRAINT c_primary_key PRIMARY KEY (checkin_id)
);

INSERT INTO ods.checkin (business_id, CHECKIN_TIMESTAMP)
SELECT 
    c.checkin_json:business_id AS business_id,
    F.value::timestamp AS checkin_timestamp
FROM 
    udacity_yelp_climate.staging.checkin c,
    TABLE(FLATTEN(split(c.checkin_json:date, ','))) F
;

SELECT * FROM ods.checkin;

-------------------------------------------------------------------------------------------------

-------------------------------------------- Dates ----------------------------------------------

CREATE OR REPLACE TABLE ods.dates(
    date_id integer PRIMARY KEY NOT NULL AUTOINCREMENT,
    date date
);

INSERT INTO ods.dates(date)
WITH dates
AS(
    SELECT 
        DISTINCT 
            to_date(DATE, 'YYYYmmdd') AS date
    FROM 
        staging.TEMPERATURE 
    UNION 
    SELECT 
        DISTINCT 
            to_date(DATE, 'YYYYmmdd') AS date
    FROM 
        staging.PRECIPITATION    
    UNION        
    SELECT 
        DISTINCT
        REVIEW_JSON:date::date AS date
    FROM
        staging.REVIEW
    UNION 
    SELECT 
        DISTINCT 
        tip_json:date::date 
    FROM 
        staging.tip
)
SELECT 
    date 
FROM 
    dates    
;

SELECT * FROM ods.dates;

-------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE ods.user_name(
    user_id varchar PRIMARY KEY,
    user_name varchar
);

INSERT INTO ods.user_name
SELECT 
    user_json:user_id::varchar AS user_id,
    user_json:name::varchar AS user_name
FROM 
    STAGING."USER" 
;

SELECT * FROM ods.user_name ;

---------------------------------------- User  --------------------------------------------------

CREATE OR REPLACE TABLE ODS.user(
    user_id varchar NOT NULL,
    review_count integer,
    yelping_since STRING,
    useful integer,
    funny integer,
    cool integer,
    fans integer,
    average_stars integer,
    compliment_hot integer,
    compliment_more integer,
    compliment_profile integer,
    compliment_cute integer,
    compliment_list integer,
    compliment_note integer,
    compliment_plain integer,
    compliment_cool integer,
    compliment_funny integer,
    compliment_writer integer,
    compliment_photos integer,
    CONSTRAINT pk_user_id PRIMARY KEY (user_id),
    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES ods.user_name(user_id)
);

INSERT INTO ods.user
SELECT 
    user_json:user_id::varchar AS user_id,
    user_json:review_count ,
    user_json:yelping_since ,
    user_json:useful ,
    user_json:funny ,
    user_json:cool ,
    user_json:fans ,
    user_json:average_stars ,
    user_json:compliment_hot ,
    user_json:compliment_more ,
    user_json:compliment_profile ,
    user_json:compliment_cute ,
    user_json:compliment_list ,
    user_json:compliment_note ,
    user_json:compliment_plain ,
    user_json:compliment_cool ,
    user_json:compliment_funny ,
    user_json:compliment_writer ,
    user_json:compliment_photos     
FROM 
    UDACITY_YELP_CLIMATE.STAGING.USER
;

SELECT * FROM ods.user;

--------------------------------------------------------------------------------------------------


------------------------------------ User Friends ------------------------------------------------

CREATE TABLE ods.friends(
    user_id varchar NOT NULL ,
    friend_id varchar,
    CONSTRAINT f_primary_key PRIMARY KEY (user_id)
);

INSERT INTO ods.friends
SELECT 
    u.user_json:user_id AS user_id,
    F.value AS friend_id
FROM 
    staging.user u,
    TABLE(
        flatten(
            split(u.user_json:friends, ',')
            )
        ) F
;

SELECT * FROM ods.friends;

----------------------------------------------------------------------------------------------------------

--------------------------------------- Year -------------------------------------------------------------

CREATE OR REPLACE TABLE ods.year(
    year_id integer PRIMARY KEY NOT NULL AUTOINCREMENT,
    YEAR integer 
);

INSERT INTO ods.year(year)
SELECT 
    DISTINCT 
    CASE 
        WHEN F.value = '20' THEN '2020'::integer
        ELSE F.value::integer
    END AS year
FROM 
    staging.USER u,
    TABLE(flatten(split(u.USER_JSON:elite, ','))) F
WHERE 
    F.value <> ''  
ORDER BY 
    1
;    

SELECT * FROM ods.YEAR;

--------------------------------------------------------------------------------------------

-----------------------------------------Elite Years ---------------------------------------

USE SCHEMA ods;

CREATE OR REPLACE TABLE ods.elite_table(
    user_id varchar NOT NULL ,
    year_id integer ,
    CONSTRAINT pk_user_id PRIMARY KEY (user_id),
    CONSTRAINT fk_year_id FOREIGN KEY (year_id) REFERENCES ods.year(year_id)
);

INSERT INTO ods.elite_table(user_id, year_id)
WITH 
elite_years AS(
    SELECT 
        u.user_json:user_id::varchar AS user_id,
        F.value::integer AS year
    FROM 
        staging.USER u,
        TABLE(flatten(split(u.USER_JSON:elite, ','))) F
    WHERE 
        F.value <> ''          
)
SELECT 
    ey.user_id,
    y.year_id
FROM 
    elite_years ey
LEFT JOIN 
    ods.YEAR y
USING
    (year)
;

SELECT * FROM ods.elite_table ;

--------------------------------------------------------------------------------------------
-------------------------------------- City ------------------------------------------------

CREATE OR REPLACE TABLE ods.city(
    city_id integer PRIMARY KEY AUTOINCREMENT,
    city_name varchar
);

INSERT INTO ods.city(city_name)
SELECT 
    DISTINCT 
    BUSINESS_JSON:city AS city_name
FROM 
    staging.BUSINESS 
;    

SELECT count(*) FROM ods.city;

--------------------------------------------------------------------------------------------

--------------------------------------------- State ----------------------------------------

CREATE TABLE ods.state(
    state_id integer PRIMARY KEY AUTOINCREMENT,
    state_name varchar
);

INSERT INTO ods.state(state_name)
SELECT 
    DISTINCT 
    BUSINESS_JSON:state::varchar AS state_name
FROM 
    staging.BUSINESS 
;

SELECT * FROM ods.state;

---------------------------------------------------------

-------------------- Address --------------------------------

CREATE TABLE ods.address(
    address_id integer PRIMARY KEY AUTOINCREMENT,
    address varchar
);

INSERT INTO ods.address(address)
SELECT 
    DISTINCT 
    BUSINESS_JSON:address::varchar AS address
FROM 
    staging.BUSINESS 
;

SELECT count(*) FROM ods.address;

--------------------------------------------------------------------------------------------------

------------------------------------------ Postal_code -------------------------------------------

CREATE TABLE ods.postal_code(
    postal_code_id integer PRIMARY KEY AUTOINCREMENT,
    postal_code varchar
);

INSERT INTO ods.postal_code(postal_code)
SELECT 
    DISTINCT 
    BUSINESS_JSON:postal_code::varchar AS postal_code
FROM 
    staging.BUSINESS 
;

SELECT * FROM ods.postal_code;

----------------------------------------- Location  -------------------------------------------

CREATE OR REPLACE TABLE ods.location(
    loc_id integer AUTOINCREMENT, 
    longitude float,
    latitude float,
    CONSTRAINT pk_loc_id PRIMARY KEY (loc_id)
);

INSERT INTO ods.location(LONGITUDE, LATITUDE)
SELECT 
    DISTINCT 
    b.BUSINESS_JSON:longitude::float AS longitude,
    b.BUSINESS_JSON:latitude::float AS latitude
FROM 
    staging.BUSINESS b
;

---------------------------------------------------------------------------------------------------------

------------------------------------------- categories --------------------------------------------------

CREATE OR REPLACE TABLE ods.category(
    category_id integer AUTOINCREMENT,
    category_name varchar,
    CONSTRAINT pk_category_id PRIMARY KEY (category_id)
);

INSERT INTO ods.category(category_name)
SELECT 
    DISTINCT 
    F.value::varchar AS category_name
FROM
    STAGING.BUSINESS b,
    TABLE(
        flatten(
            split(b.business_json:categories, ',')
            )
        ) F
;

SELECT count(*) FROM ods.category;

----------------------------------- business name --------------------------------------------------------

CREATE OR REPLACE TABLE ods.business_name(
    business_id varchar PRIMARY KEY ,
    business_name varchar
);

INSERT INTO ods.business_name 
SELECT 
    b.BUSINESS_JSON:business_id::varchar ,
    b.BUSINESS_JSON:name
FROM 
    staging.BUSINESS b
;

SELECT * FROM ods.BUSINESS_name ;

---------------------------------------- business ---------------------------------------------------------

CREATE OR REPLACE TABLE ods.business(
    business_id varchar,
    address_id integer,
    city_id integer,
    postal_code_id integer, 
    state_id integer,
    loc_id integer,
    CONSTRAINT pk_business_id PRIMARY KEY (business_id),
    CONSTRAINT fk_address_id FOREIGN KEY (address_id) REFERENCES ods.address(address_id),
    CONSTRAINT fk_city_id FOREIGN KEY (city_id) REFERENCES ods.city(city_id),
    CONSTRAINT fk_postal_code_id FOREIGN KEY (postal_code_id) REFERENCES ods.postal_code(postal_code_id),
    CONSTRAINT fk_state_id FOREIGN KEY (state_id) REFERENCES ods.state(state_id),
    CONSTRAINT fk_loc_id FOREIGN KEY (loc_id) REFERENCES ods.location(loc_id)
);


INSERT INTO ods.business
SELECT 
    BUSINESS_JSON:business_id::varchar AS business_id,
    a.address_id,
    c.city_id,
    p.postal_code_id,
    s.state_id,
    l.loc_id
FROM 
    staging.BUSINESS b
LEFT JOIN
    ods.address a 
ON 
    b.BUSINESS_JSON:address = a.address
LEFT JOIN 
    ods.city c
ON 
    b.BUSINESS_JSON:city = c.city_name
LEFT JOIN 
    ods.postal_code p
ON 
    b.BUSINESS_JSON:postal_code = p.postal_code
LEFT JOIN 
    ods.state s 
ON 
    b.BUSINESS_JSON:state::varchar = s.STATE_NAME 
LEFT JOIN 
    ods.location l
ON 
    (ROUND(b.BUSINESS_JSON:longitude::float, 4) = ROUND(l.longitude,4)
     AND 
     ROUND(b.BUSINESS_JSON:latitude::float, 4) = ROUND(l.latitude::float,4)
     )
;

SELECT * FROM ods.business;

--------------------------------------------- business categories ------------------------------------------

CREATE OR REPLACE TABLE ods.business_categories(
    business_id varchar PRIMARY KEY ,
    category_id integer,
    CONSTRAINT fk_business_id FOREIGN KEY (business_id) REFERENCES ods.business(business_id),
    CONSTRAINT fk_category_id FOREIGN KEY (category_id) REFERENCES ods.category(category_id)
);

INSERT INTO ods.business_categories
WITH business_cats_map AS(
    SELECT 
        b.BUSINESS_JSON:business_id AS business_id, 
        F.value::varchar AS category_name
    FROM
        STAGING.BUSINESS b,
        TABLE(
            flatten(
                split(b.business_json:categories, ',')
                )
            ) F
)
SELECT 
    bcm.business_id,
    c.category_id
FROM 
    business_cats_map bcm
LEFT JOIN 
    ods.category c
ON 
    bcm.category_name = c.category_name
;

SELECT * FROM ods.BUSINESS_CATEGORIES;

-------------------------------------------------------------------------------------------------------------

--------------------------------------------- business hours ------------------------------------------------

CREATE TABLE ods.business_opening_hours(
    business_id varchar,
    day_of_week varchar,
    opening_hour time,
    closing_hour time,
    CONSTRAINT pk_business_dow_id PRIMARY KEY (business_id, day_of_week),
    CONSTRAINT fk_business_id FOREIGN KEY (business_id) REFERENCES ods.business(business_id)
);

INSERT INTO business_opening_hours 
SELECT 
    b.BUSINESS_JSON:business_id,
    'Monday' AS day_of_week, 
    split_part(get_path(b.BUSINESS_JSON:hours, 'Monday'), '-', 1)::time  AS opening_hour,
    split_part(get_path(b.BUSINESS_JSON:hours, 'Monday'), '-', 2)::time  AS closing_hour
FROM 
    STAGING.BUSINESS b   
UNION 
SELECT 
    b.BUSINESS_JSON:business_id,
    'Tuesday' AS day_of_week, 
    split_part(get_path(b.BUSINESS_JSON:hours, 'Tuesday'), '-', 1)::time  AS opening_hour,
    split_part(get_path(b.BUSINESS_JSON:hours, 'Tuesday'), '-', 2)::time  AS closing_hour
FROM 
    STAGING.BUSINESS b   
UNION 
SELECT 
    b.BUSINESS_JSON:business_id,
    'Wednesday' AS day_of_week, 
    split_part(get_path(b.BUSINESS_JSON:hours, 'Wednesday'), '-', 1)::time  AS opening_hour,
    split_part(get_path(b.BUSINESS_JSON:hours, 'Wednesday'), '-', 2)::time  AS closing_hour
FROM 
    STAGING.BUSINESS b   
UNION   
SELECT 
    b.BUSINESS_JSON:business_id,
    'Thursday' AS day_of_week, 
    split_part(get_path(b.BUSINESS_JSON:hours, 'Thursday'), '-', 1)::time  AS opening_hour,
    split_part(get_path(b.BUSINESS_JSON:hours, 'Thursday'), '-', 2)::time  AS closing_hour
FROM 
    STAGING.BUSINESS b   
UNION 
SELECT 
    b.BUSINESS_JSON:business_id,
    'Friday' AS day_of_week, 
    split_part(get_path(b.BUSINESS_JSON:hours, 'Friday'), '-', 1)::time  AS opening_hour,
    split_part(get_path(b.BUSINESS_JSON:hours, 'Friday'), '-', 2)::time  AS closing_hour
FROM 
    STAGING.BUSINESS b   
UNION 
SELECT 
    b.BUSINESS_JSON:business_id,
    'Saturday' AS day_of_week, 
    split_part(get_path(b.BUSINESS_JSON:hours, 'Saturday'), '-', 1)::time  AS opening_hour,
    split_part(get_path(b.BUSINESS_JSON:hours, 'Saturday'), '-', 2)::time  AS closing_hour
FROM 
    STAGING.BUSINESS b     
UNION 
SELECT 
    b.BUSINESS_JSON:business_id,
    'Sunday' AS day_of_week, 
    split_part(get_path(b.BUSINESS_JSON:hours, 'Sunday'), '-', 1)::time  AS opening_hour,
    split_part(get_path(b.BUSINESS_JSON:hours, 'Sunday'), '-', 2)::time  AS closing_hour
FROM 
    STAGING.BUSINESS b     
;

SELECT * FROM ods.business_opening_hours ;

-----------------------------------------------------------------------------------------------------------    

-------------------------------------- business attributes ------------------------------------------------

CREATE OR REPLACE TABLE ods.business_attributes(
    business_id varchar PRIMARY KEY, 
    stars integer, 
    review_count integer, 
    is_open integer,
    BusinessAcceptsCreditCards varchar,
    RestaurantsPriceRange2 varchar,
    BikeParking varchar,
    OutdoorSeating varchar,
    BusinessAcceptsBitcoin varchar,
    Alcohol varchar,
    RestaurantsGoodForGroups varchar,
    Caters varchar,
    RestaurantsReservations varchar,
    DogsAllowed varchar,
    RestaurantsTableService varchar,
    AcceptsInsurance varchar,
    RestaurantsTakeOut varchar,
    Wifi varchar,
    HasTv varchar,
    DriveTru varchar,
    AgesAllowed varchar,
    Open24Hours varchar,
    Smoking varchar,
    GoodForDancing varchar,
    ByAppointmentOnly varchar,
    WheelchairAccessible varchar,
    RestaurantsDelivery varchar,
    GoodForkids varchar,
    NoiseLevel varchar,
    Ambience varchar,
    RestaurantsAttire varchar,
    HappyHour varchar,
    CoatCheck varchar,
    RestaurantsCounterService varchar,
    Corkage varchar,
    BYOB varchar,
    BYOBcorkage varchar,
    DietaryRestrictons varchar,
    BusinessparkingStreet boolean,
    BusinessParkingValidated boolean,
    AmbienceClassy boolean,
    AmbiencyDivey boolean,
    AmbienceUpscale boolean,
    GoodForMealDesert boolean,
    GoodForMealDinner boolean,
    GoodForMealLatenight boolean,
    BestNightsFriday  boolean,
    BestNightsSunday boolean,
    AmbienceRomantic boolean,
    BestNightsMonday boolean,
    HairSpecializesInColoring  boolean,
    HairSpecializesInCurly  boolean,
    HairSpecializesInExtensions  boolean,
    HairSpecializesInKids  boolean,
    HairSpecializesInPerms  boolean,
    AmbienceIntimate boolean,
    AmbienceCasual boolean,
    AmbienceHipster boolean,
    AmbienceTouristy boolean,
    AmbienceTrendy boolean,
    BusinessParkingValet boolean,
    GoodForMealLunch boolean,
    HairSpecializesInAfricanAmerican boolean,
    HairSpecializesInAsian boolean,
    HairSpecializesInStraightPerms boolean,
    BestNightsThursday boolean,
    BestNightsTuesday boolean,
    BusinessParkingGarage boolean,
    GoodForMealBreakfast boolean,
    GoodForMealBrunch boolean,
    BestNightsWednesday boolean,
    BusinessParkingLot boolean,
    BestNightsSaturday boolean,
    MusicDj boolean,
    MusicJudebox boolean,
    MusicNo_Music boolean,
    MusicVideo boolean,
    MusicBackground_Music boolean,
    MusicKaraoke boolean,
    MusicLive boolean
);

INSERT INTO ods.business_attributes
WITH 
attributes_direct AS(
    SELECT 
        b.BUSINESS_JSON:business_id::varchar AS business_id,
        b.BUSINESS_JSON:stars::integer AS stars,
        b.BUSINESS_JSON:review_count::integer AS review_count,
        b.BUSINESS_JSON:is_open::integer AS is_open
    FROM
        staging.BUSINESS b
)
,
attributes_simple AS(
    SELECT 
        b.BUSINESS_JSON:business_id::varchar AS business_id,
        atts.KEY::varchar AS key,
        atts.value::varchar AS value 
    FROM 
        staging.BUSINESS b,
        lateral flatten(input => parse_json(b.BUSINESS_JSON:attributes), outer => TRUE) atts
    WHERE 
        atts.key NOT IN  (
            'BusinessParking',
            'BestNights', 
            'Ambience',
            'HairSpecializesIn', 
            'GoodForMeal',
            'Music'
        )
)
,
attributes_simple_wide AS(
    SELECT 
        * 
    FROM 
        attributes_simple  
    pivot(
        max(value) FOR KEY IN (
                'BusinessAcceptsCreditCards',
                'RestaurantsPriceRange2',
                'BikeParking',
                'OutdoorSeating',
                'BusinessAcceptsBitcoin',
                'Alcohol',
                'RestaurantsGoodForGroups',
                'Caters',
                'RestaurantsReservations',
                'DogsAllowed',
                'RestaurantsTableService',
                'AcceptsInsurance',
                'RestaurantsTakeOut',
                'WiFi',
                'HasTV',
                'DriveThru',
                'AgesAllowed',
                'Open24Hours',
                'Smoking',
                'GoodForDancing',
                'ByAppointmentOnly',
                'WheelchairAccessible',
                'RestaurantsDelivery',
                'GoodForKids',
                'NoiseLevel',
                'Ambience',
                'RestaurantsAttire',
                'HappyHour',
                'CoatCheck',
                'RestaurantsCounterService',
                'Corkage',
                'BYOB',
                'BYOBCorkage',
                'DietaryRestrictions'
        )
    )
    ORDER BY 
        business_id
)
,
attributes_complex AS(
    SELECT 
        b.BUSINESS_JSON:business_id::varchar AS business_id,
        (atts.KEY || INITCAP(child_atts.KEY))::varchar AS key,        
        child_atts.value::boolean AS value
    FROM 
        staging.BUSINESS b,
        LATERAL flatten(input => parse_json(b.BUSINESS_JSON:attributes),
                        outer => TRUE) atts,
        LATERAL flatten(TRY_PARSE_JSON(atts.value),
                        OUTER => TRUE) child_atts
    WHERE 
        atts.key IN  (
            'BusinessParking',
            'BestNights', 
            'Ambience',
            'HairSpecializesIn', 
            'GoodForMeal',
            'Music'
        )               
)
,
attributes_complex_wide AS(
    SELECT 
        * 
    FROM
        attributes_complex 
    pivot(
        max(value) FOR KEY IN (
                'BusinessParkingStreet',
                'BusinessParkingValidated',
                'AmbienceClassy',
                'AmbienceDivey',
                'AmbienceUpscale',
                'GoodForMealDessert',
                'GoodForMealDinner',
                'GoodForMealLatenight',
                'BestNightsFriday',
                'BestNightsSunday',
                'AmbienceRomantic',
                'BestNightsMonday',
                'HairSpecializesInColoring',
                'HairSpecializesInCurly',
                'HairSpecializesInExtensions',
                'HairSpecializesInKids',
                'HairSpecializesInPerms',
                'AmbienceIntimate',
                'AmbienceCasual',
                'AmbienceHipster',
                'AmbienceTouristy',
                'AmbienceTrendy',
                'BusinessParkingValet',
                'GoodForMealLunch',
                'HairSpecializesInAfricanamerican',
                'HairSpecializesInAsian',
                'HairSpecializesInStraightperms',
                'BestNightsThursday',
                'BestNightsTuesday',
                'BusinessParkingGarage',
                'GoodForMealBreakfast',
                'GoodForMealBrunch',
                'BestNightsWednesday',
                'BusinessParkingLot',
                'BestNightsSaturday',
                'MusicDj',
                'MusicJukebox',
                'MusicNo_Music',
                'MusicVideo',
                'MusicBackground_Music',
                'MusicKaraoke',
                'MusicLive'
        )
    )
    ORDER BY business_id
)
,
final_table AS(
    SELECT
        *
    FROM
        attributes_direct ad
    LEFT OUTER JOIN 
        attributes_simple_wide asw
    USING
        (business_id)
    LEFT OUTER JOIN 
        attributes_complex_wide acw
    USING
        (business_id)
)
SELECT * FROM final_table
;

SELECT * FROM ods.business_attributes;


----------------------------------------------------------------------------------------------------------------------

----------------------------------------------- covid features -------------------------------------------------------

CREATE OR REPLACE TABLE ods.covid_features(
    business_id varchar PRIMARY KEY,
    call_to_action_enabled varchar,
    covid_banner varchar,
    grubhub_enabled varchar,
    request_a_quote_enabled varchar,
    temporary_closed_until varchar,
    virtual_services_offered varchar,
    delivery_or_takeout varchar,
    highlights varchar,
    YEAR varchar,
    number varchar,
    CONSTRAINT fk_business_id FOREIGN KEY (business_id) REFERENCES ods.business(business_id)
);

INSERT INTO ods.covid_features
WITH 
simple_covid_features AS(
    SELECT  
        cf.COVID_FEATURES_JSON:business_id::varchar AS business_id,
        cf.COVID_FEATURES_JSON:"Call To Action enabled"::varchar AS call_to_action_enabled,
        cf.COVID_FEATURES_JSON:"Covid Banner"::varchar AS covid_banner,
        cf.COVID_FEATURES_JSON:"Grubhub enabled"::varchar AS grubhub_enabled,    
        cf.COVID_FEATURES_JSON:"Request a Quote Enabled"::varchar AS request_a_quote_enabled,        
        cf.COVID_FEATURES_JSON:"Temporary Closed Until"::varchar AS temporary_closed_until,   
        cf.COVID_FEATURES_JSON:"Virtual Services Offered"::varchar AS virtual_services_offered, 
        cf.COVID_FEATURES_JSON:"delivery or takeout"::varchar AS delivery_or_takeout
    FROM 
        staging.COVID_FEATURES cf
)
,
complex_covid_features AS(
    SELECT
        cf.COVID_FEATURES_JSON:business_id AS business_id,
        child_atts.KEY AS key,
        child_atts.value AS value  
    FROM 
        staging.COVID_FEATURES cf,
        LATERAL flatten(input => TRY_PARSE_JSON(cf.COVID_FEATURES_JSON:"highlights"), outer => TRUE) highlights, 
        LATERAL flatten(TRY_PARSE_JSON(highlights.value),  OUTER => TRUE) child_atts
    WHERE 
        child_atts.KEY IS NOT NULL 
)
, intermediate AS(
    SELECT 
        *
    FROM
        complex_covid_features 
    pivot(
        max(value) FOR KEY IN ('identifier', 'params', 'type')
    )
    AS P(business_id, identifier, params, highlight)
    ORDER BY 
        business_id 
)
,
intermediate_2 AS(
    SELECT 
       i.business_id,
       i.highlight AS highlight,
       F.KEY AS key,
       F.value AS value 
    FROM 
        intermediate i,
        LATERAL flatten(INPUT => try_parse_json(i.params), OUTER => TRUE) F
)
, intermediate_3 AS(
    SELECT 
       *
    FROM 
        intermediate_2
    pivot(
        max(value) FOR KEY IN ('year', 'number')        
    )
    AS 
        P(business_id, highlight, YEAR, number)
    ORDER BY 
        (business_id, highlight)
)
SELECT 
    sf.*,
    i3.highlight,
    i3.YEAR,
    i3.number
FROM
    simple_covid_features sf
LEFT JOIN 
    intermediate_3  i3
USING 
    (business_id)
;

SELECT * FROM ods.covid_features;


-----------------------------------------------------------------------------------------------------------

----------------------------------------------- Tip -------------------------------------------------------

CREATE OR REPLACE TABLE ods.tip(
    tip_id integer NOT NULL PRIMARY KEY AUTOINCREMENT,
    business_id varchar NOT NULL,
    user_id varchar NOT NULL,
    date_id integer NOT NULL,
    text varchar, 
    compliment_count integer,
    CONSTRAINT fk_business_id FOREIGN KEY (business_id)
        REFERENCES ods.business(business_id),
    CONSTRAINT fk_user_id FOREIGN KEY (user_id) 
        REFERENCES ods.user_name(user_id),
    CONSTRAINT fk_date_id FOREIGN KEY (date_id) 
        REFERENCES ods.dates(date_id)
);

INSERT INTO ods.tip(business_id, user_id, date_id, TEXT, compliment_count)
SELECT 
    tip_json:business_id AS business_id,
    tip_json:user_id AS user_id,
    d.date_id AS date_id,
    tip_json:text::varchar AS text,
    tip_json:compliment_count AS compliment_count
FROM 
    UDACITY_YELP_CLIMATE.STAGING.TIP t
LEFT JOIN 
    ods.dates d
ON 
    tip_json:date::date = d.date
;

SELECT * FROM UDACITY_YELP_CLIMATE.ODS.TIP;

---------------------------------------------------------------------------------------------------------

------------------------------------------------ review -------------------------------------------------

CREATE OR REPLACE TABLE ods.review(
    review_id varchar NOT NULL PRIMARY KEY ,
    user_id varchar,
    business_id varchar, 
    date_id integer,
    text varchar ,
    useful integer,
    funny integer,
    cool integer ,
    CONSTRAINT fk_user_id FOREIGN KEY (user_id)
        REFERENCES ods.user_name(user_id),
    CONSTRAINT fk_business_id FOREIGN KEY (business_id) 
        REFERENCES ods.business(business_id),
    CONSTRAINT fk_date_id FOREIGN KEY (date_id) 
        REFERENCES ods.dates(date_id)  
);

INSERT INTO ods.review
SELECT 
    review_json:review_id::varchar AS review_id,
    review_json:user_id::varchar AS user_id,
    review_json:business_id::varchar AS business_id,
    d.date_id AS date_id,
    review_json:text AS text,
    review_json:useful::integer AS useful,
    review_json:funny::integer AS funny,
    review_json:cool::integer AS cool
FROM 
    staging.REVIEW r
LEFT JOIN 
    ods.dates d
ON 
    r.REVIEW_JSON:date::date = d.date 
;    

SELECT 
    *
FROM
    UDACITY_YELP_CLIMATE.ODS.review
;

------------------------------------------------ Weather ------------------------------------------------- 

CREATE OR REPLACE TABLE ods.precipitation(
    date_id integer,
    postal_code_id integer,    
    precipitation_inches float, 
    precipitation_normal_inches float,
    CONSTRAINT fk_date_id FOREIGN KEY(date_id) REFERENCES ods.dates(date_id),
    CONSTRAINT fk_postal_code_id FOREIGN KEY(postal_code_id)  REFERENCES ods.postal_code(postal_code_id)
);

INSERT INTO ods.precipitation
SELECT 
    D.date_id,
    po.postal_code_id,
    TRY_CAST(PRECIPITATION_INCHES AS float) AS precipitation_inches ,
    TRY_CAST(PRECIPITATION_NORMAL_INCHES AS float) AS precipitation_normal_inches
FROM 
    staging.PRECIPITATION pre
LEFT JOIN 
    ods.dates d
ON 
    to_date(pre.DATE, 'YYYYmmdd') = D.date 
LEFT JOIN 
    ods.postal_code po
ON 
    pre.POSTAL_CODE = po.POSTAL_CODE 
;

SELECT * FROM ods.precipitation;

CREATE OR REPLACE TABLE ods.temperature(
    date_id integer,
    postal_code_id integer,
    min_fahr float, 
    max_fahr float,
    normal_min_fahr float,
    normal_max_fahr float,
    CONSTRAINT fk_date_id FOREIGN KEY(date_id) REFERENCES ods.dates(date_id),
    CONSTRAINT fk_postal_code_id FOREIGN KEY(postal_code_id)  REFERENCES ods.postal_code(postal_code_id)
);

INSERT INTO ods.temperature
SELECT 
    D.date_id,
    po.POSTAL_CODE_ID ,
    TRY_CAST(min_fahr AS float) AS min_fahr ,
    TRY_CAST(max_fahr AS float) AS max_fahr,
    normal_min_fahr::float AS normal_min_fahr,
    normal_max_fahr::float AS normal_max_fahr
FROM 
    staging.TEMPERATURE t
LEFT JOIN 
    ods.dates d
ON 
    to_date(t.DATE, 'YYYYmmdd') = D.date 
LEFT JOIN 
    ods.postal_code po
ON 
    t.POSTAL_CODE = po.POSTAL_CODE 
;
    
SELECT * FROM ods.TEMPERATURE;

---------------------------------------------------------------------------------------------------------- 

---------------------------------------------------------------------------------------------------------- 

-----------------------------------------------Sizes -----------------------------------------------------

LIST @my_json_stage;
LIST @my_csv_stage;
SHOW TABLES IN staging;
SHOW TABLES IN ods;
 