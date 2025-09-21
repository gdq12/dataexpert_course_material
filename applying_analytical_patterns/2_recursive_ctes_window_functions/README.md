### Get started with the lab

1. Spin up the containers from set up [README](../../setup_postgres/README.md)

    ```bash
    ~/git_repos/dataexpert_intermediate_bootcamp_2025/1_dimensional_data_modeling/1_setup_postgres

    make up
    ```

2. Go to PGadmin UI: [http://localhost:5050](http://localhost:5050) 

3. Go to server from the setup session. If starting from scratch follow setup instructions in [README](../1_setup_postgres/README.md)

### Creating a web events stat tbl 

```sql
create table web_events_dashboard as 
with combined as 
(select 
	case 
		when referrer ~* 'zachwilson|eczachly|dataengineerr.io'
			then 'Onsite'
		when referrer ~* 't.co'
			then 'Twitter'
		when referrer ~* 'linkedin' 
			then 'LinkedIn'
		when referrer ~* 'instagram'
			then 'Instagram'
		when referrer is null 
			then 'Direct'
		else 'Other'
	end referrer_mapped,
	e.*,
	coalesce(d.browser_type, 'N/A') browser_type,
	coalesce(d.os_type, 'N/A') os_type
from events e 
join devices d on e.device_id = d.device_id
-- where url = '/signup'
) 
select 
	coalesce(referrer_mapped, 'Overall') referrer,
	coalesce(browser_type, 'Overall') browser_type,
	coalesce(os_type, 'overall') os_type,
	count(1) number_of_site_hits,
	count(case 
			when url = '/signup'
				then 1
			end) number_of_signup_visits,
	count(case 
			when url = '/contact'
				then 1
			end) number_of_contact_visits,
	count(case 
			when url = '/login'
				then 1
			end) number_of_login_visits,
	count(case 
			when url = '/signup'
				then 1
			end)::real/count(1) conversion_rate
from combined
group by grouping sets (
	(referrer_mapped, browser_type, os_type),
	(os_type),
	(browser_type),
	()
)
-- having count(1) > 100 -- to filter out group by that had only a few (like 1) user and therefor mistakenly perceived as 100% conversion
-- order by count(case 
-- 			when url = '/signup'
-- 				then 1
-- 			end) desc
;
```

+ additional info:

    - `referrer_mapped` was created to reduce the cadinality of referrer for the group by group sets ()

    - `coalesce()` is used here a lot for the group by group set(). in the last part of the query its to fill up `null` for the records/col combos where the group by was smaller (like os_type and browser_type). Coalesce was also use to fill in empty os_type and browser_type so that in the final results, the record wouldnt be filled as Overall and be incorrectly counted in the wrong group by, aka ().

    - an alternative to the group by group set() is `group by rollup (referrer_mapped, browser_type, os_type)`, this will return the same results as above except excude records where referrer_mapped is overall since it wont run the () group by.

    - group by rollup is always best with hierarichal data 

    - using `group by cube(referrer_mapped, browser_type, os_type)` will produce the most results since it will try to perform all possible group by permutations 


### Following user activity in a given session 

**session defined as a day for the purpose of the lab**

* EDA query on tracing user visit to the website in a given session/day
    ```sql
    with combined as 
    (select 
        distinct case 
            when referrer ~* 'zachwilson|eczachly|dataengineerr.io'
                then 'Onsite'
            when referrer ~* 't.co'
                then 'Twitter'
            when referrer ~* 'linkedin' 
                then 'LinkedIn'
            when referrer ~* 'instagram'
                then 'Instagram'
            when referrer is null 
                then 'Direct'
            else 'Other'
        end referrer_mapped,
        e.*,
        coalesce(d.browser_type, 'N/A') browser_type,
        coalesce(d.os_type, 'N/A') os_type
    from events e 
    join devices d on e.device_id = d.device_id
    where e.user_id = '439578290726747300'
    ) 
    select 
        c1.user_id,
        c1.url, 
        c2.url,
        c1.event_time,
        c2.event_time,
        (c1.event_time::timestamp - c2.event_time::timestamp) delta_time
    from combined c1 
    join combined c2 on c1.user_id = c2.user_id 
    and date(c1.event_time) = date(c2.event_time) -- this is to define a single session 
    and c1.event_time > c2.event_time
    order by c1.user_id, c1.event_time, c2.event_time
    ;
    ```

    + additional notes: 

        - there seems to be many duplicates in the events table so did a distinct 

        - `and date(c1.event_time) = date(c2.event_time)` is used to define a session

        - `and c1.event_time > c2.event_time` + the self join is used to see the jumping around the pages in a given session. This can be done by using a window function, but doing the self join method to demo the equivalent of that 

        - can look at the `delta_time` and `url` to see if its a bot visit or not. a bot will have mm second delta times while a human will have minutes/hours/seconds delta_time

* examine stats of user journey through an app/webpage

    ```sql
    with dedup as 
    (select 
        e.*,
        d.*,
        row_number() over (partition by e.user_id, d.device_id, d.browser_type, d.os_type, e.event_time
                            order by e.event_time) row_num
    from events e 
    join devices d on e.device_id = d.device_id
    ), 
    combined as 
    (select 
        *,
        case 
            when referrer ~* 'zachwilson|eczachly|dataengineerr.io'
                then 'Onsite'
            when referrer ~* 't.co'
                then 'Twitter'
            when referrer ~* 'linkedin' 
                then 'LinkedIn'
            when referrer ~* 'instagram'
                then 'Instagram'
            when referrer is null 
                then 'Direct'
            else 'Other'
        end referrer_mapped,
        coalesce(browser_type, 'N/A') browser_type,
        coalesce(os_type, 'N/A') os_type
    from dedup
    where row_num = 1
    -- and user_id = '439578290726747300'
    ),
    aggregated as 
    (select 
        c1.user_id,
        c1.url to_url, 
        c2.url from_url,
        min(c1.event_time::timestamp - c2.event_time::timestamp) min_delta_time,
        max(c1.event_time::timestamp - c2.event_time::timestamp) max_delta_time
    from combined c1 
    join combined c2 on c1.user_id = c2.user_id 
    and date(c1.event_time) = date(c2.event_time) -- this is to define a single session 
    and c1.event_time > c2.event_time
    group by 1, 2, 3
    )
    select 
        from_url,
        to_url,
        count(1) num_users,
        min(min_delta_time) min_duration,
        max(min_delta_time) max_duration,
        avg(min_delta_time) avg_duration
    from aggregated 
    group by 1, 2
    having count(1) > 100 -- to filter out insignificant cohort groups 
    ;
    ```

    + additional notes: 

        - this query groups user_ids into cohorts based on the to and from url pages they visit 

        - it tries to look at the webpage usage pattern by looking at how fast or slow a given cohorts reaches the destination url 

    + implications:

        - this useful EDA can be used for examining user behavior pattern and how the current app or website design influences them

        - deeper stats can be conducted like decile/percentile categorization to see the distribution of behavior

        - before and after A/B stats can be conducted to see how a UI change in a given app changes the user behavior to more targetted/favorable or not