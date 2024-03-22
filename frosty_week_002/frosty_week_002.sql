------------------------------------------------------------
-- Frosty Friday Week 2
-- https://frostyfriday.org/blog/2022/07/15/week-2-intermediate/
------------------------------------------------------------
------------------------------------------------------------
-- Load in the parquet data and transform it into a table, 
-- then create a stream that will only show us changes to the DEPT and JOB_TITLE columns.
------------------------------------------------------------

-- initial setups 
use database frosty_challenges;
create or replace schema week_2;
use schema week_2;

-- creating a file format
create or replace file format parquet_ff
    type=parquet;

-- creating a stage 
create or replace stage w2_stage;

-- putting a file into a stage using snowsql
-- put file:///Users/feruzkholov/Downloads/employees.parquet @frosty_challenges.week_2.w2_stage;

-- check the schema of the file 
select*from table(
    infer_schema(
        location=> '@w2_stage',
        file_format=> 'parquet_ff'
    )
);

-- creating a table 
create or replace table w2_table (
    employee_id int primary key,
    first_name varchar,
    last_name varchar,
    email varchar,
    street_num int,
    street_name varchar,
    city varchar,
    postcode varchar,
    country varchar,
    country_code varchar,
    time_zone varchar,
    payroll_iban varchar,
    dept varchar,
    job_title varchar,
    education varchar,
    title varchar,
    suffix varchar
);

-- copying data into w2_table
copy into w2_table from @w2_stage
    file_format = parquet_ff
    match_by_column_name = case_insensitive;

-- creating a view for a stream
create or replace view track_changes
as select
    employee_id,
    dept,
    job_title
from w2_table;

-- creating a stream from the view
create or replace stream w2_stream on view track_changes;

-- updating some data in the main table
UPDATE w2_table SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
UPDATE w2_table SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
UPDATE w2_table SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
UPDATE w2_table SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
UPDATE w2_table SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;

-- checking the result
select * from track_changes;
select * from w2_stream;