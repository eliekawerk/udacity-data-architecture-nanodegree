-----------------------------------------------DWH ---------------------------------------------------------------

-----------------------------------------------Star Schema -------------------------------------------------------

CREATE TABLE DATAWAREHOUSE.dim_review(
    review_id varchar PRIMARY KEY ,
    text varchar,
    useful integer,
    funny integer, 
    cool integer 
);

INSERT INTO DATAWAREHOUSE.dim_review
SELECT 
    review_id::varchar,
    TEXT::varchar,
    useful::integer,
    funny::integer, 
    cool::integer
FROM 
    ods.review
;

CREATE TABLE DATAWAREHOUSE.dim_date(
    date_id integer PRIMARY KEY,
    date date 
);

INSERT INTO DATAWAREHOUSE.dim_date
SELECT 
    date_id::integer,
    date::date
FROM 
    ods.DATES
;

SELECT * FROM DATAWAREHOUSE.dim_date;

CREATE TABLE DATAWAREHOUSE.dim_user(
    user_id varchar PRIMARY KEY,
    user_name varchar,
    yelping_since date,
    fans integer,
    average_stars float
);

INSERT INTO DATAWAREHOUSE.dim_user 
SELECT 
    u.user_id::varchar,
    un.USER_NAME::varchar,
    u.YELPING_SINCE::date,
    u.fans::integer,
    u.AVERAGE_STARS::float 
FROM 
    ods.USER u
LEFT JOIN 
    ods.user_name un
USING
    (user_id)
;

SELECT * FROM DATAWAREHOUSE.dim_user;

CREATE TABLE datawarehouse.dim_business(
    business_id varchar PRIMARY KEY , 
    business_name varchar ,
    address varchar,
    city varchar,
    state varchar,
    longitude float,
    latitude float
);

INSERT INTO datawarehouse.dim_business
SELECT 
    b.BUSINESS_ID ,
    bn.BUSINESS_NAME ,
    a.ADDRESS ,
    c.CITY_NAME ,
    s.STATE_NAME ,
    loc.LONGITUDE ,
    loc.LATITUDE 
FROM 
    ods.BUSINESS b
LEFT JOIN 
    ods.BUSINESS_NAME bn
USING
    (business_id)
LEFT JOIN 
    ods.ADDRESS a
USING 
    (address_id)
LEFT JOIN 
    ods.city c
USING 
    (city_id)    
LEFT JOIN 
    ods.state s
USING 
    (state_id)    
LEFT JOIN 
    ods.LOCATION loc
USING 
    (loc_id)
;

SELECT * FROM DATAWAREHOUSE.dim_business;

CREATE OR REPLACE TABLE datawarehouse.fact_reviews(
    review_id varchar PRIMARY KEY,
    date_id integer,
    user_id varchar,
    business_id varchar ,
    min_temp_fahr float,
    max_temp_fahr float,
    precipitation float,
    CONSTRAINT fk_review_id FOREIGN KEY (review_id) REFERENCES datawarehouse.dim_review(review_id),
    CONSTRAINT fk_date_id FOREIGN KEY (date_id) REFERENCES datawarehouse.dim_date(date_id),
    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES datawarehouse.dim_user(user_id),    
    CONSTRAINT fk_business_id FOREIGN KEY (business_id) REFERENCES datawarehouse.dim_business(business_id)    
);

INSERT INTO datawarehouse.fact_reviews
WITH 
weather AS(
    SELECT 
        T.POSTAL_CODE_ID ,
        T.DATE_ID ,
        T.MIN_FAHR AS min_temp_fahr ,
        T.MAX_FAHR AS max_temp_fahr,
        prec.PRECIPITATION_INCHES 
    FROM 
        ods.TEMPERATURE T
    INNER JOIN 
        ods.PRECIPITATION prec
    USING
        (postal_code_id, date_id)
)
,
reviews AS(
    SELECT 
        r.REVIEW_ID ,
        r.DATE_ID ,
        b.POSTAL_CODE_ID,
        r.user_id,
        r.BUSINESS_ID    
    FROM 
        ods.review r 
    LEFT JOIN 
        ods.BUSINESS b
    USING 
        (business_id)
)
SELECT 
    r.review_id,
    r.date_id,
    r.user_id,
    r.business_id,
    w.min_temp_fahr,
    w.max_temp_fahr,
    w.precipitation_inches
FROM 
    reviews r
JOIN 
    WEATHER w
USING 
    (postal_code_id, date_id)
;    

SELECT * FROM DATAWAREHOUSE.fact_reviews;

------------------------------------------------------------------------------------------------------------------
