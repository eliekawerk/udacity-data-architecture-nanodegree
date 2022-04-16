CREATE TABLE education_level(
    edu_lvl_id SERIAL primary key,
    edu_lvl VARCHAR(50)
);

INSERT INTO education_level(edu_lvl)
SELECT DISTINCT education_lvl
FROM proj_stg;

CREATE TABLE email(
    email_id SERIAL primary key,
    email VARCHAR(50)
);

INSERT INTO email(email)
SELECT DISTINCT email FROM proj_stg;

CREATE TABLE hire_dt(
    hire_dt_id SERIAL primary key,
    hire_dt DATE
);

INSERT INTO hire_dt(hire_dt)
SELECT DISTINCT hire_dt FROM proj_stg;


CREATE TABLE employee(
    emp_id VARCHAR(50) primary key,
    emp_nm VARCHAR(50),
    email_id INT references email(email_id),
    hire_dt_id INT references hire_dt(hire_dt_id),
    edu_lvl_id INT references education_level(edu_lvl_id)
);

INSERT INTO employee(emp_id, emp_nm, email_id, hire_dt_id, edu_lvl_id)
SELECT DISTINCT
    ps.emp_id,
    ps.emp_nm,
    em.email_id,
    hd.hire_dt_id,
    el.edu_lvl_id
FROM
    proj_stg ps
JOIN
    education_level el
ON
    ps.education_lvl = el.edu_lvl
JOIN
    email em
ON
    ps.email = em.email
JOIN
    hire_dt hd
ON
    ps.hire_dt = hd.hire_dt
;

CREATE TABLE job_title(
    job_ttl_id SERIAL primary key,
    job_ttl VARCHAR(50)
);

INSERT INTO job_title(job_ttl)
SELECT DISTINCT job_title FROM proj_stg;

CREATE TABLE department(
    dep_id SERIAL primary key,
    dep_nm VARCHAR(50)
);

INSERT INTO department(dep_nm)
SELECT DISTINCT department_nm FROM proj_stg;

CREATE TABLE location(
    loc_id SERIAL primary key,
    loc_nm VARCHAR(50)
);

INSERT INTO location(loc_nm)
SELECT DISTINCT location FROM proj_stg;

CREATE TABLE state(
    state_id SERIAL primary key,
    state_nm VARCHAR(50),
    loc_id int references location(loc_id)
);

INSERT INTO state(state_nm, loc_id)
SELECT DISTINCT
    ps.state,
    loc.loc_id
FROM
    proj_stg ps
JOIN
    location loc
ON
    ps.location = loc.loc_nm
;

CREATE TABLE city(
    city_id SERIAL primary key,
    city_nm VARCHAR(50),
    state_id int references state(state_id)
);

INSERT INTO city(city_nm, state_id)
SELECT DISTINCT
    ps.city,
    st.state_id
FROM
    proj_stg ps
JOIN
    state st
ON
    ps.state = st.state_nm
;

CREATE TABLE address(
    adrss_id SERIAL primary key,
    adrss VARCHAR(50),
    city_id int references city(city_id)
);

INSERT INTO address(adrss, city_id)
SELECT DISTINCT
    ps.address,
    c.city_id
FROM
    proj_stg ps
JOIN
    city c
ON
    ps.city = c.city_nm
;

CREATE TABLE emp_hstry(
    emp_hstry_id serial primary key,
    emp_id VARCHAR(50) references employee(emp_id),
    job_ttl_id int references job_title(job_ttl_id) ,
    dep_id int references department(dep_id),
    mngr_id VARCHAR(50) references employee(emp_id),
    adrss_id int references address(adrss_id),
    start_dt DATE ,
    end_dt DATE
);

INSERT INTO emp_hstry(emp_id, job_ttl_id, dep_id, mngr_id, adrss_id, start_dt, end_dt)
SELECT
    ps.emp_id,
    j.job_ttl_id,
    d.dep_id,
    e.emp_id AS mngr_id,
    a.adrss_id ,
    ps.start_dt,
    ps.end_dt
FROM
    proj_stg ps
JOIN
    job_title j
ON
    ps.job_title = j.job_ttl
JOIN
    department d
ON
    ps.department_nm = d.dep_nm
JOIN
    employee e
ON
    ps.manager = e.emp_nm
JOIN
    address a
ON
    ps.address = a.adrss
;


CREATE TABLE salary(
    emp_hstry_id INT primary key references emp_hstry(emp_hstry_id),
    salary NUMERIC
);

INSERT INTO salary(emp_hstry_id, salary)
SELECT
    eh.emp_hstry_id,
    ps.salary
FROM
    proj_stg ps
JOIN
    employee e
ON
    ps.emp_nm = e.emp_nm
JOIN
    job_title j
ON
    ps.job_title = j.job_ttl
JOIN
    department d
ON
    ps.department_nm = d.dep_nm
JOIN
    emp_hstry eh
ON
    e.emp_id = eh.emp_id
AND
    j.job_ttl_id = eh.job_ttl_id
AND
    d.dep_id = eh.dep_id
AND
    ps.start_dt = eh.start_dt
;

----
-- sanity check

select * from emp_hstry;
SELECT * FROM employee;
SELECT * FROM job_title;
select * from email;
select * from department;
select * from address;
select * from city;
select * from location ;
select * from salary ;
select * from state;
select * from hire_dt;
select * from education_level;

------

-- 1 job titles

SELECT
    e.emp_nm,
    j.job_ttl,
    d.dep_nm
FROM
    emp_hstry
JOIN
    employee e
USING
    (emp_id)
JOIN
    department d
USING
    (dep_id)
JOIN
    job_title j
USING
    (job_ttl_id)
;

-- Question 2
SELECT * FROM job_title;

INSERT INTO job_title(job_ttl) VALUES
('Web Programmer')

UPDATE
    job_title
SET
    job_ttl = 'Web Developer'
WHERE
    job_ttl = 'Web Programmer'
;

DELETE FROM
    job_title
WHERE
    job_ttl = 'Web Developer'
;

-- QUestion 5

SELECT
    d.dep_nm,
    COUNT(DISTINCT eh.emp_id)
FROM
    emp_hstry eh
JOIN
    department d
ON
    d.dep_id = eh.dep_id
GROUP BY
    1
ORDER BY
    2
;


-- Question 6
SELECT
    eh.emp_hstry_id,
    e.emp_nm,
    j.job_ttl,
    d.dep_nm,
    eh.start_dt,
    eh.end_dt ,
    s.salary
FROM
    emp_hstry eh
JOIN
    employee e
USING
    (emp_id)
JOIN
    department d
USING
    (dep_id)
JOIN
    job_title j
USING
    (job_ttl_id)
JOIN
    salary s
USING
    (emp_hstry_id)
WHERE
    e.emp_nm = 'Toni Lembeck'
;

-- Section 4

CREATE OR REPLACE VIEW employee_initial AS
SELECT
    eh.emp_id ,
    e.emp_nm ,
    em.email ,
    hd.hire_dt ,
    el.edu_lvl ,
    j.job_ttl,
    sal.salary,
    d.dep_nm,
    m.emp_nm AS manager,
    eh.start_dt,
    eh.end_dt,
    a.adrss ,
    c.city_nm ,
    s.state_nm,
    l.loc_nm
FROM
    emp_hstry eh
JOIN
    employee e
ON
    eh.emp_id = e.emp_id
JOIN
    email em
ON
    e.email_id = em.email_id
JOIN
    hire_dt hd
ON
    e.hire_dt_id = hd.hire_dt_id
JOIN
    education_level el
ON
    e.edu_lvl_id = el.edu_lvl_id
JOIN
    job_title j
ON
    eh.job_ttl_id = j.job_ttl_id
JOIN
    salary sal
ON
    eh.emp_hstry_id = sal.emp_hstry_id
JOIN
    department d
ON
    eh.dep_id = d.dep_id
JOIN
    employee m
ON
    eh.mngr_id = m.emp_id
JOIN
    address a
ON
    eh.adrss_id = a.adrss_id
JOIN
    city c
ON
    a.city_id = c.city_id
JOIN
    state s
ON
    c.state_id = s.state_id
JOIN
    location l
ON
    s.loc_id = l.loc_id
;

-- Question 2

CREATE OR REPLACE FUNCTION query_all_history(employee_name TEXT)
returns TABLE(emp_nm TEXT, job_ttl TEXT, dep_nm TEXT, mngr TEXT, start_dt date, end_dt date)
AS
$$
    SELECT
        e.emp_nm,
        j.job_ttl,
        d.dep_nm,
        m.emp_nm AS manager,
        eh.start_dt,
        eh.end_dt
    FROM
        emp_hstry eh
    JOIN
        (SELECT * FROM employee WHERE emp_nm = employee_name) e
    ON
        eh.emp_id = e.emp_id
    JOIN
        job_title j
    ON
        eh.job_ttl_id = j.job_ttl_id
    JOIN
        department d
    ON
        eh.dep_id = d.dep_id
    JOIN
        employee m
    ON
        eh.mngr_id = m.emp_id
$$
LANGUAGE SQL;

-- Question 3 - data security

CREATE ROLE NoMgr LOGIN;
REVOKE ALL ON DATABASE postgres FROM NoMgr;
GRANT CONNECT ON DATABASE postgres TO NoMgr;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO NoMgr;
REVOKE ALL PRIVILEGES ON salary FROM NoMgr;