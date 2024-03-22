------------------------------------------------------------
-- Frosty Friday Week 1
-- https://frostyfriday.org/2022/07/14/week-1/
------------------------------------------------------------
------------------------------------------------------------
-- create an external stage from s3 bucket s3://frostyfridaychallenges/challenge_1/, 
-- and load the csv files directly from that stage into a table.
------------------------------------------------------------

-- setting up
create or replace database frosty_challenges;

use database frosty_challenges;

create or replace schema week_1;

use schema week_1;

-- creating an external stage 
create or replace stage week_1_stage
    url = 's3://frostyfridaychallenges/challenge_1/'
    comment='External stage for week 1 challenge';

list @week_1_stage;

-- exploring data
select 
    $1 as col1,
    metadata$filename as file_id,
    metadata$file_row_number as row_num
from @week_1_stage;

-- create file format
create or replace file format ff_w1_format
    skip_header=1
    type='CSV'
    field_delimiter=',';

-- creating table 
create or replace table week_1(
    sample_col varchar
);

-- loading data from stage into a table 
copy into week_1
from @week_1_stage
pattern='.*[1-3].csv'
file_format=(format_name=ff_w1_format);

select*from week_1;