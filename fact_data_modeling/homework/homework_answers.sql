-- A query to deduplicate game_details from Day 1 so there's no duplicates
with dedup as 
(select 
	*,
	row_number() over(partition by game_id, team_id, player_id
						order by coalesce(pts, 0)
						) row_num
from game_details 
) 
select * 
from dedup
where row_num = 1
;

-- A DDL for an user_devices_cumulated table
create table user_devices_cumulated (
	user_id text,
	browser_type text,
	device_activity_datelist date[],
	date date,
	primary key (user_id, browser_type, date)
)
;

-- A cumulative query to generate device_activity_datelist from events
insert into user_devices_cumulated
with vars as 
(select date('2023-01-05') as yesterday, date('2023-01-06') as today
), 
yesterday as 
(select 
	user_id,
	browser_type,
	device_activity_datelist,
	date
from user_devices_cumulated
where date = (select yesterday from vars)
),
today as 
(select 
	e.user_id::text as user_id,
	coalesce(d.browser_type, 'Not Provided') as browser_type,
	date(e.event_time::date) dates_active
from events as e 
left join devices as d on e.device_id = d.device_id
where date(e.event_time::date) = (select today from vars)
and e.user_id is not null 
group by e.user_id::text,
		coalesce(d.browser_type, 'Not Provided'),
		date(e.event_time::date)
) 
select 
    coalesce(t.user_id, y.user_id::text) user_id,
	coalesce(t.browser_type, y.browser_type) browser_type,
    case 
        when y.device_activity_datelist is null 
            then array[t.dates_active]
        when t.dates_active is null 
            then y.device_activity_datelist
        else array[t.dates_active]||y.device_activity_datelist
    end dates_active,
    coalesce(t.dates_active, y.date + interval '1 day') date
from today t 
full outer join yesterday y on t.user_id = y.user_id::text 
							and t.browser_type = y.browser_type
;

-- A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
with users as 
(select 
	user_id,
	browser_type,
	device_activity_datelist,
	date
from user_devices_cumulated
where date = date('2023-01-02')
),
series as 
(select * 
from generate_series(date('2023-01-01'), date('2023-01-06'), interval '1 day') series_date
),
place_holder_ints as 
(select 
    case 
        -- is the series date within the engagement date list 
        when device_activity_datelist @> array[date(series_date)]
            -- the number of days between current date and series date to the power of 2
            then pow(2, 32 - (date - date(series_date)))::bigint
        else 0
        -- ::bit(32) --> turns the calc value into a bit value (takes up less memory)
        end placeholder_int_value,
    *
from user_devices_cumulated
cross join series 
) 
select 
    user_id,
	browser_type,
    (sum(placeholder_int_value)::bigint)::bit(32) as datelist_int
from place_holder_ints
group by user_id,
		browser_type
;

-- A DDL for hosts_cumulated table
create table hosts_cumulated (
	host text,
	host_activity_datelist date[],
	date date,
	primary key (host, date)
)
;

-- The incremental query to generate host_activity_datelist
insert into hosts_cumulated
with vars as 
(select date('2023-01-05') as yesterday, date('2023-01-06') as today
), 
yesterday as 
(select 
	host,
	host_activity_datelist,
	date
from hosts_cumulated
where date = (select yesterday from vars)
),
today as 
(select 
	e.host::text as host,
	date(e.event_time::date) dates_active
from events as e 
where date(e.event_time::date) = (select today from vars)
and e.user_id is not null 
group by e.host::text,
		date(e.event_time::date)
) 
select 
    coalesce(t.host, y.host::text) host,
    case 
        when y.host_activity_datelist is null 
            then array[t.dates_active]
        when t.dates_active is null 
            then y.host_activity_datelist
        else array[t.dates_active]||y.host_activity_datelist
    end dates_active,
    coalesce(t.dates_active, y.date + interval '1 day') date
from today t 
full outer join yesterday y on t.host = y.host::text 
;

-- A monthly, reduced fact table DDL host_activity_reduced
create table host_activity_reduced (
    month_start date,
    host text,
    hit_array int[],
    unique_visitors int[],
    primary key (month, host)
)
;

-- An incremental query that loads host_activity_reduced
insert into host_activity_reduced
with vars as 
(select date('2023-01-03') as date_var, date('2023-01-01') as month_var
), 
daily_aggregate as 
(select
    host::text as host,
    date(event_time) as date,
    count(1) as num_site_hits,
	count(distinct user_id::text) as num_users
from events
where date(event_time) = (select date_var from vars)
and user_id is not null
group by host::text,
    	date(event_time)
),
yesterday_aggregate as 
(select 
	month_start,
    host,
    hit_array,
    unique_visitors
from host_activity_reduced
where date(month_start) = (select month_var from vars)
)
select 
    coalesce(ya.month_start, date_trunc('month', da.date)) month_start,
	coalesce(da.host, ya.host) host,
    case 
        when ya.hit_array is not null 
            then ya.hit_array||array[coalesce(da.num_site_hits, 0)]
        else array_fill(0, array[coalesce(da.date-coalesce(ya.month_start, date(date_trunc('month', da.date))), 0)])
            ||array[coalesce(da.num_site_hits, 0)]
    end hit_array,
	case 
        when ya.unique_visitors is not null 
            then ya.unique_visitors||array[coalesce(da.num_users, 0)]
        else array_fill(0, array[coalesce(da.date-coalesce(ya.month_start, date(date_trunc('month', da.date))), 0)])
            ||array[coalesce(da.num_users, 0)]
    end unique_visitors
from daily_aggregate da 
full outer join yesterday_aggregate ya on da.host = ya.host
on conflict (month_start, host)
do
	update set hit_array = excluded.hit_array, unique_visitors = excluded.unique_visitors
;
