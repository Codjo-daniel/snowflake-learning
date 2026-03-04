create database HOTEL_DB;

-- load data manually using the UI
create file format ff_csv
    type = 'CSV'
    field_optionally_enclosed_by = '"'
    skip_header = 1
    null_if = ('NULL', 'null', '')

create or replace stage STG_HOTEL_BOOKING
    file_format = ff_csv

-- Create the bronze table to ingest the raw data from stage
create table bronze_hotel_booking (
    booking_id string,
    hotel_id string,
    hotel_city string,	
    customer_id	string,
    customer_name string,	
    customer_email string,	
    check_in_date string,	
    check_out_date string,	
    room_type string,
    num_guests string,
    total_amount string,
    currency string,
    booking_status string
)

-- load the data from the file to the bronze table using copy into command
copy into bronze_hotel_booking
from @stg_hotel_booking
file_format = (format_name = ff_csv)
on_error = 'continue'

-- query the table
select * from bronze_hotel_booking limit 10;

-- Let's create the silver table
create table silver_hotel_booking(
    booking_id varchar,
    hotel_id varchar,
    hotel_city varchar,
    customer_id varchar,
    customer_name varchar,
    customer_email varchar,
    check_in_date date,
    check_out_date date,
    room_type varchar,
    num_guests integer,
    total_amount float,
    currency varchar,
    booking_status varchar
)
-- Check non valid email
select customer_email  --- 401 rows
from bronze_hotel_booking
where not (customer_email like '%@%.%')
or customer_email is null

-- Check non valid total amount -- 176 rows
select total_amount
from bronze_hotel_booking
where try_to_number(total_amount) < 0;
-- where try_cast(total_amount as number) < 0;

-- Parse negative value
with cte as (
select
    case
        when try_cast(total_amount as float) < 0 then try_cast(total_amount as float) * -1
        else try_cast(total_amount as float)
    end as total_amount
from bronze_hotel_booking)
select count(*) from cte --where total_amount < 0

-- check if there are check-in date greater than checkout date 271 rows
with cte_invalid as (
select 
    check_in_date,
    check_out_date,
    case
        when datediff(day,try_cast(check_in_date as date), try_cast(check_out_date as date)) > 0 then 1
        else 0
    end as is_valid_check_in_check_out      
from bronze_hotel_booking)

select * from cte_invalid
where is_valid_check_in_check_out = 0
-- Much better Make a hole date analysis. After analysis there are invalid date check-in-date column like 31-02-2024 greater than check out date
with cte1 as (
select
    check_in_date,
    check_out_date,
    case
        when try_cast(check_in_date as date) < try_cast(check_out_date as date)
        then 1 else 0
    end as is_valid_date
from bronze_hotel_booking
where is_valid_date = 0),

cte2 as( -- 71
    select
        check_in_date,
        check_out_date
    from bronze_hotel_booking
    where try_to_date(check_in_date) > try_to_date(check_out_date)
)

(select 
    check_in_date,
    check_out_date
from cte1
except
select * from cte2)
-- union all
(select * from cte2
except
select 
    check_in_date,
    check_out_date
from cte1)

-- booking status distinct value
select distinct booking_status 
from bronze_hotel_booking;
-- where booking_status is null; --Confirmeeed

--check room_type
select distinct room_type
from  bronze_hotel_booking

-- transform and load the data into silver table
insert into silver_hotel_booking
select
    try_cast(booking_id as  varchar) booking_id,  
    try_cast(hotel_id as  varchar) hotel_id,  
    try_cast(initcap(trim(hotel_city)) as  varchar) hotel_city,  
    try_cast(customer_id as  varchar) customer_id,  
    try_cast(initcap(trim(customer_name)) as  varchar) customer_name,
    case
        when not (customer_email like '%@%.%') or customer_email is null then null
        else lower(trim(customer_email))
        end as customer_email,  
    try_cast(check_in_date as  date) check_in_date,  
    try_cast(check_out_date as  date) check_out_date,  
    try_cast(room_type as  varchar) room_type,  
    try_cast(num_guests as  integer) num_guests,
    abs(try_cast(total_amount as float)) as total_amount,
    lower(currency) as currency,
    case
        when lower(booking_status) = 'confirmeeed' then 'confirmed'
        else lower(booking_status)
    end as booking_status
from bronze_hotel_booking
where try_to_date(check_in_date) is not null
and try_to_date(check_out_date) is not null
and try_to_date(check_out_date) >= try_to_date(check_in_date)
-- where datediff(day,check_in_date, check_out_date) > 0

-- select check_in_date from bronze_hotel_booking
-- where try_cast(check_out_date as date) = '2024-12-12'

--- query silver table
select * from silver_hotel_booking as
where check_out_date is null --sample(10 ROWS)

-- delete from silver_hotel_booking

-- Aggreagate data
create table gold_agg_daily_booking as
select
    check_in_date as date,
    count(*) as total_booking,
    sum(total_amount) as total_revenue
from silver_hotel_booking
group by check_in_date
order by date

-- Hotel city sales
create table gold_agg_hotel_city_sales as
select
    hotel_city,
    sum(total_amount) as total_revenue_by_city
from silver_hotel_booking
group by hotel_city
order by total_revenue_by_city desc

create table gold_hotel_booking as
select * from silver_hotel_booking

select distinct