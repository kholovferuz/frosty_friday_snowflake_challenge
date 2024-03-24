------------------------------------------------------------
-- Frosty Friday Week 3
-- https://frostyfriday.org/blog/2022/07/15/week-3-basic/
------------------------------------------------------------
------------------------------------------------------------
-- Create a table that lists all the files in our stage that 
-- contain any of the keywords in the keywords.csv file.
------------------------------------------------------------

-- initial setups
use database frosty_challenges;
create or replace schema week_3;
use schema week_3;

-- creating a file format
create or replace file format w3_csv
type='CSV'
field_delimiter='|'
skip_header=1;

-- creating an s3 stage 
create or replace stage w3_stage 
    url='s3://frostyfridaychallenges/challenge_3/'
    file_format = w3_csv
    comment='A stage for week 3 challenge';

list @w3_stage;

-- checking distinct filenames in the stage 
select 
distinct metadata$filename,
from @w3_stage
order by metadata$filename ;

-- checking the column names and values in the keywords.csv file
select 
metadata$filename, 
metadata$file_row_number,
$1
from @w3_stage
where  metadata$filename='challenge_3/keywords.csv';

-- creating a table for keywords
create or replace table keywords(
    keyword varchar,
    added_by varchar,
    nonsense varchar
);

-- loading data into keywords table
copy into  keywords
from @w3_stage/keywords.csv
file_format=(field_delimiter=',', skip_header=1);

select*from keywords;

-- creating the final result table
create or replace table result_table
as
select 
metadata$filename filename, count(*) number_of_rows,
from @w3_stage
where  metadata$filename!='challenge_3/keywords.csv'
and metadata$filename like any (select '%'||keyword||'%' from keywords)
group by filename;

select*from result_table
order by 2;

