show databases;
use SNOWFLAKE_SAMPLE_DATA;
show schemas;
use schema TPCH_SF1;
show tables;

--- fetch customer metadata
describe table customer;
--- Select a subset of a table
select
c.c_name, c.c_comment from customer c limit 5;

select split(c.c_name, '#')[1] from customer c limit 3;

---- Using with clause

with split_cte as (
    select split(c.c_name, '#') as c_name_split from customer c limit 3
)

select cast(c_name_split[0] as string) as customer_name, cast(c_name_split[1] as integer) as customer_id
from split_cte
-- select all columns of the table

select * from customer sample(10 rows)

select concat(c.c_phone, 'OK') as c_phone_process from customer c limit 4;

show tables;
--- trying to use case when and with statement
with integer_date as (
    select l_shipdate, dayofweek(l_shipdate) as l_day, month(l_shipdate) as l_month from lineitem sample(10)
)

select
    l_shipdate,
    case
        when l_day = 0 then 'Sunday'
        when l_day = 1 then 'Monday'
        when l_day = 2 then 'Tuesday'
        when l_day = 3 then 'Wednesday'
        when l_day = 4 then 'Thursday'
        when l_day = 5 then 'Friday'
        when l_day = 6 then 'Saturday'
        else 'Not a valid Date'
    end as l_day,
    case
        when l_month = 1 then 'January'
        when l_month = 2 then 'February'
        when l_month = 3 then 'March'
        when l_month = 4 then 'April'
        when l_month = 5 then 'May'
        when l_month = 6 then 'June'
        when l_month = 7 then 'July'
        when l_month = 8 then 'August'
        when l_month = 9 then 'September'
        when l_month = 10 then 'October'
        when l_month = 11 then 'November'
        when l_month = 12 then 'December'
    end as l_month,
    case
        when l_month in (1, 2, 3) then 1
        when l_month in (4, 5, 6) then 2
        when l_month in (7,8,9) then 3
        when l_month in (10, 11,12) then 4
    end as l_trimester,
    case
        when l_month in (1,2,3,4) then 1
        when l_month in (5,6,7,8) then 2
        when l_month in (9,10,11,12) then 3
    end as l_quarter,
    case
        when l_month in (1,2,3,4,5,6) then 1
        when l_month in (7,8,9,10,11,12) then 2
    end as l_semester
from integer_date;

-- join statement
select count(*) from lineitem --- 6.001.215;
select count(*) from orders; --- 1.500.000
select * from part limit 5
select * from customer limit 5;
select * from supplier limit 5

---
describe table lineitem
select * from lineitem limit 5

-- Compute total revenue per order
select 
    o.o_orderkey,
    -- l_extendedprice,
    -- l_discount,
    sum(l.l_extendedprice * (1 - l.l_discount)) as revenue_per_orders,  
from lineitem l
join orders o
on o.o_orderkey = l.l_orderkey
group by o.o_orderkey
limit 6
