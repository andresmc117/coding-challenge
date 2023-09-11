--Postgres database
CREATE OR REPLACE TABLE IF NOT EXISTS departments(
    id INTEGER PRIMARY KEY,
    department VARCHAR
);

CREATE OR REPLACE TABLE IF NOT EXISTS jobs(
    id INTEGER PRIMARY KEY,
    job VARCHAR
);

CREATE OR REPLACE TABLE IF NOT EXISTS hired_employees(
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    datetime VARCHAR,
    department_id INTEGER,
    job_id INTEGER,
    CONSTRAINT fk_departments
      FOREIGN KEY(department_id) 
	    REFERENCES departments(id),
    CONSTRAINT fk_jobs
      FOREIGN KEY(job_id) 
	    REFERENCES jobs(id)
);

-- DELETE FROM hired_employees;
-- DELETE FROM departments;
-- DELETE FROM jobs;

--Query 1
WITH date_converted AS (
    SELECT
        department_id,
        job_id,
        TO_TIMESTAMP(REPLACE(REPLACE(datetime, 'T', ' '), 'Z', ''), 'YYYY-MM-DD HH24:MI:SS') AS datetime
    FROM hired_employees
    WHERE department_id = 3
),
get_quarter AS (
    SELECT
        department_id,
        job_id,
        datetime,
        CASE
            WHEN EXTRACT('Month' FROM datetime) BETWEEN 1 AND 3 THEN 1
            ELSE 0
        END AS Q1,
        CASE
            WHEN EXTRACT('Month' FROM datetime) BETWEEN 4 AND 6 THEN 1
            ELSE 0
        END AS Q2,
        CASE
            WHEN EXTRACT('Month' FROM datetime) BETWEEN 7 AND 9 THEN 1
            ELSE 0
        END AS Q3,
        CASE
            WHEN EXTRACT('Month' FROM datetime) BETWEEN 10 AND 12 THEN 1
            ELSE 0
        END AS Q4
    FROM date_converted
)
SELECT
    d.department,
    j.job,
    SUM(Q1) AS Q1,
    SUM(Q2) AS Q2,
    SUM(Q3) AS Q3,
    SUM(Q4) AS Q4
FROM get_quarter AS a
LEFT JOIN departments d ON a.department_id = d.id
LEFT JOIN jobs j ON a.job_id = j.id
WHERE EXTRACT('Year' FROM datetime) = 2021
GROUP BY d.department, j.job
ORDER BY d.department, j.job;


--Query 2
WITH date_converted AS (
    SELECT
        department_id,
        job_id,
        TO_TIMESTAMP(REPLACE(REPLACE(datetime, 'T', ' '), 'Z', ''), 'YYYY-MM-DD HH24:MI:SS') AS datetime
    FROM hired_employees
), hires AS (
    SELECT
        EXTRACT('Year' FROM datetime) AS year,
        department_id,
        COUNT(1) AS hired
    FROM date_converted
    GROUP BY year, department_id
), avg_2021 AS (
    SELECT
        AVG(hired) AS avg_hired_2021
    FROM hires
    WHERE year = 2021
)
SELECT
    h.department_id,
    d.department,
    h.hired
FROM hires AS h
LEFT JOIN departments d ON d.id = h.department_id
INNER JOIN avg_2021 AS a ON true
WHERE h.hired > a.avg_hired_2021
ORDER BY h.hired DESC
;


