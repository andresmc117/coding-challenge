from connectors.postgres_connection import PostgresHRConnector

def execute_query(query):
    """Creates a postgres connection and executes a SQL script.

    :param query: sql script to execute
    """
    try:
        postgres_conn = PostgresHRConnector()
        postgres_conn.connect()
        result = postgres_conn.fetch_all(query)
        return result
    except:
        print("error")
    finally:
        postgres_conn.disconnect()


def get_eployees_per_quarter():
    query = """
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
            department_id,
            job_id,
            SUM(Q1) AS Q1,
            SUM(Q2) AS Q2,
            SUM(Q3) AS Q3,
            SUM(Q4) AS Q4
        FROM get_quarter
        WHERE EXTRACT('Year' FROM datetime) = 2021
        GROUP BY department_id, job_id
        ORDER BY department_id, job_id;


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
    """
    return execute_query(query)

def get_eployees_hired_per_avg_dep():
    query = """
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
    """
    return execute_query(query)