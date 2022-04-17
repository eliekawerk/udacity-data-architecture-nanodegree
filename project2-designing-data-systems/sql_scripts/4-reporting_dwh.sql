
---------------------------------------------SQL Reporting -------------------------------------------------------

/*
* Report generating for each business and date:
* - the number of unique users
* - the average min and max temperatures
* - the average number of useful comments
* - the average number of funny comments
*/


SELECT 
    b.business_name,
    d.date,
    count(DISTINCT user_id) AS num_unique_users, 
    avg(fr.min_temp_fahr) AS avg_min_temp_fahr,
    avg(fr.max_temp_fahr) AS avg_max_temp_fahr,
    avg(r.useful) AS avg_useful,
    avg(r.funny) AS avg_funny
FROM 
    DATAWAREHOUSE.fact_reviews fr
LEFT JOIN 
    DATAWAREHOUSE.dim_business b
USING 
    (business_id)
LEFT JOIN 
    DATAWAREHOUSE.dim_date d
USING
    (date_id)
LEFT JOIN 
    DATAWAREHOUSE.dim_review r 
USING 
    (review_id)
GROUP BY 
    1, 2
;

------------------------------------------------------------------------------------------------------------------
