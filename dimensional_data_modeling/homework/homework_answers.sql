-- 1. DDL for actor table 
create type films as (
    film text, 
    votes integer,
    rating real,
    filmid text,
    year integer
)
;

create type quality_class as enum ('star', 'good', 'average', 'bad');

create table actors (
    actor_id text,
    actor_name text,
    films films[],
    quality_class quality_class, 
    is_active boolean,
    creation_year integer,
    primary key(actor_id, films, creation_year)
)
;

-- 2. Cumulative table generation query
insert into actors
with vars as 
(select 2009 as last_year, 2010 as this_year
), 
last_year as 
(select *
from actors 
where creation_year = (select last_year from vars)
),
this_year as 
(select 
	actorid,
	actor,
	year,
	array_agg(row(
					film,
					votes,
					rating,
					filmid,
					year
				)::films) as films,
	case 
		when avg(rating) > 8 
			then 'star'
		when avg(rating) > 7 and avg(rating) <= 8
			then 'good'
		when avg(rating) > 6 and avg(rating) <= 7
			then 'average'
		when avg(rating) <= 6
			then 'bad'
	end::quality_class as quality_class
from actor_films
where year = (select this_year from vars)
group by 1, 2, 3
),
nested as 
(select 
	coalesce(t.actorid, y.actor_id) as actor_id,
	coalesce(t.actor, y.actor_name) as actor_name, 
	case 
		when y.films is null 
			then t.films
		when t.films is not null 
			then y.films||t.films
		else y.films
	end as films,
	coalesce(t.year, y.creation_year + 1) as creation_year,
	t.year is not null as is_active,
	coalesce(t.quality_class, y.quality_class) as quality_class
from this_year as t 
full outer join last_year as y on t.actorid = y.actor_id 
							and t.actor = y.actor_name
) 
select 
	actor_id,
	actor_name,
	films,
	quality_class,
	is_active,
	creation_year
from nested
;

-- 3.DDL for actors_history_scd table
create table actors_history_scd (
	actor_id text,
    actor_name text,
    films films[],
    quality_class quality_class, 
    is_active boolean,
	start_year integer,
	end_year integer,
    creation_year integer,
    primary key(actor_id, start_year, end_year, is_active)
)
;

-- 4.Backfill query for actors_history_scd
insert into actors_history_scd
with vars as 
(select 2009 as target_year
), 
with_previous as 
(select 
    actor_id,
	actor_name, 
    films,
	quality_class,
	is_active,
	creation_year,
    lag(quality_class, 1) over (partition by actor_id order by creation_year) as previous_quality_class,
    lag(is_active, 1) over (partition by actor_id order by creation_year) as previous_is_active
from actors
where creation_year <= (select target_year from vars)
),
with_indicators as
(select 
    *,
    case
        when quality_class <> previous_quality_class then 1 
        when is_active <> previous_is_active then 1 
    else 0
    end as change_indicator
from with_previous
),
with_streaks as 
(select 
    *, 
    sum(change_indicator) over (partition by actor_id order by creation_year) as streak_indicator 
from with_indicators
)
select 
    actor_id,
	actor_name, 
    films,
	quality_class,
	is_active,
    min(year) as start_year,
    max(year) as end_year,
    (select target_year from vars) as creation_year
from with_streaks, 
unnest(films) WITH ORDINALITY as u(film, votes, rating, filmid, year) 
group by actor_id,
	actor_name, 
    films,
	quality_class,
	is_active
order by actor_id,
		start_year
;

-- 5.Incremental query for actors_history_scd
create type scd_type as (
	quality_class quality_class, 
	is_active boolean,
	start_year integer,
	end_year integer
)
;

insert into actors_history_scd
with vars as 
(select 2009 as last_year, 2010 as this_year
), 
last_year_scd as 
(select * 
from actors_history_scd
where creation_year = (select last_year from vars)
and end_year = (select last_year from vars)
),
historical_scd as 
(select 
    actor_id,
	actor_name,
	films,
    quality_class,
    is_active,
    start_year,
    end_year
from actors_history_scd
where creation_year = (select last_year from vars)
and end_year < (select last_year from vars)
),
this_year_data as 
(select 
	actor_name,
	actor_id, 
	films,
	quality_class,
	is_active,
	creation_year, 
	min(u.year) start_year,
	max(u.year) end_year
from actors,
unnest(films) as u
where creation_year = (select this_year from vars)
group by 1,2,3,4,5,6
),
unchanged_records as
(select 
	ty.actor_id,
    ty.actor_name, 
	ty.films,
    ty.quality_class, 
    ty.is_active,
    ty.start_year,
    ty.end_year 
from this_year_data ty 
join last_year_scd ly on ty.actor_name = ly.actor_name 
					and ty.actor_id = ly.actor_id
where ly.quality_class = ty.quality_class
and ly.is_active = ty.is_active 
),
new_records as 
(select 
	ty.actor_id, 
	ty.actor_name, 
	ty.films,
    ty.quality_class, 
    ty.is_active,
    ty.start_year,
    ty.end_year
from this_year_data ty 
left join last_year_scd ly on ty.actor_name = ly.actor_name 
							and ty.actor_id = ly.actor_id
where ly.actor_id is null 
and ty.end_year = (select this_year from vars)
), 
changed_records as 
(select 
    ty.actor_id, 
	ty.actor_name,
	ty.films,
    unnest(array[
            row(ly.quality_class, 
            ly.is_active,
            ly.start_year,
            ly.end_year
            )::scd_type,
            row(ty.quality_class, 
            ty.is_active,
            ty.start_year,
            ty.end_year
            )::scd_type
        ]) records
from this_year_data ty
left join last_year_scd ly on ty.actor_name = ly.actor_name 
							and ty.actor_id = ly.actor_id
where (ly.quality_class <> ty.quality_class
or ly.is_active <> ty.is_active)
), 
unnested_changed_records as 
(select 
    actor_id,
	actor_name,
	films,
    (records::scd_type).quality_class,
    (records::scd_type).is_active,
    (records::scd_type).start_year,
    (records::scd_type).end_year
from changed_records
)
select *, (select this_year from vars) current_season
from historical_scd
union all 
select *, (select this_year from vars) current_season
from unchanged_records
union all 
select *, (select this_year from vars) current_season
from unnested_changed_records
union all 
select *, (select this_year from vars) current_season
from new_records
;