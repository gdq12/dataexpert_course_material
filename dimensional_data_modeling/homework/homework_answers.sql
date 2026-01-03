-- starting from scratch 
drop table actors;
drop table actors_history_scd;
drop type scd_type;
drop type quality_class;
drop type films;


-- 1. DDL for actor table 
create type films as (
    film text, 
    votes integer,
    rating real,
    filmid text,
    year integer
)
;

create type quality_class as enum ('star', 'good', 'average', 'bad', 'unknown');

create table actors (
    actor_id text not null,
    actor_name text,
    films films[],
    quality_class quality_class not null default 'unknown', 
    is_active boolean not null,
    creation_year integer not null,
    primary key(actor_id, creation_year)
)
;

-- 2. Cumulative table generation query
insert into actors
with vars as 
(select 2005 as last_year, 2006 as this_year
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
group by actorid,
        actor,
        year
),
nested as 
(select 
	coalesce(t.actorid, y.actor_id) as actor_id,
	coalesce(t.actor, y.actor_name) as actor_name, 
	coalesce(t.films, y.films) as films,
	coalesce(t.year, y.creation_year + 1) as creation_year,
	t.year is not null as is_active,
	coalesce(t.quality_class, y.quality_class) as quality_class
from this_year as t 
full outer join last_year as y on t.actorid = y.actor_id 
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
	actor_id text not null,
    actor_name text,
    quality_class quality_class not null default 'unknown', 
    is_active boolean not null,
	start_year integer not null,
	end_year integer not null,
    creation_year integer not null,
	primary key(actor_id, start_year, creation_year)
)
;

-- 4.Backfill query for actors_history_scd
insert into actors_history_scd
with vars as 
-- -1 so can simulate incremental loading of the tbl as well 
(select max(creation_year)-1 as target_year from actors
), 
with_previous as 
(select 
    actor_id,
	actor_name, 
	quality_class,
	is_active,
	creation_year,
    lag(quality_class, 1) over (partition by actor_id order by creation_year) as previous_quality_class,
    lag(is_active, 1) over (partition by actor_id order by creation_year) as previous_is_active
from actors
where creation_year <= (select target_year from vars)
),
with_streaks as
(select 
    actor_id,
	actor_name, 
	quality_class,
	is_active,
	creation_year,
    sum(case
            when quality_class <> previous_quality_class then 1 
            when is_active <> previous_is_active then 1 
        else 0
        end) as streak_indicator
from with_previous
group by actor_id,
	actor_name, 
	quality_class,
	is_active,
	creation_year
)
select 
    actor_id,
	actor_name, 
	quality_class,
	is_active,
    min(creation_year) as start_year,
    max(creation_year) as end_year,
    (select target_year from vars) as creation_year
from with_streaks
group by actor_id,
	actor_name, 
	quality_class,
	is_active,
    streak_indicator
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
(select 
	max(creation_year)-1 as last_year,
	max(creation_year) as this_year
from actors
), 
last_year_scd as 
(select 
	actor_id,
	actor_name,
    quality_class,
    is_active,
    start_year,
    end_year,
	creation_year
from actors_history_scd
where creation_year = (select last_year from vars)
and end_year = (select last_year from vars)
),
historical_scd as 
(select 
    actor_id,
	actor_name,
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
	quality_class,
	is_active,
	creation_year
from actors
where creation_year = (select this_year from vars)
),
unchanged_records as
(select 
	ty.actor_id,
    ty.actor_name, 
    ty.quality_class, 
    ty.is_active,
	ly.start_year,
    ty.creation_year as end_year
from this_year_data ty 
join last_year_scd ly on ty.actor_id = ly.actor_id
					and ly.quality_class = ty.quality_class
					and ly.is_active = ty.is_active 
),
new_records as 
(select 
	ty.actor_id, 
	ty.actor_name, 
    ty.quality_class, 
    ty.is_active,
    ty.creation_year as start_year,
	ty.creation_year as end_year
from this_year_data ty 
left join last_year_scd ly on ty.actor_id = ly.actor_id
where ly.actor_id is null 
), 
changed_records as 
(select 
    ty.actor_id, 
	ty.actor_name,
    unnest(array[
            row(ly.quality_class, 
            ly.is_active,
            ly.start_year,
            ly.end_year
            )::scd_type,
            row(ty.quality_class, 
            ty.is_active,
            ty.creation_year,
            ty.creation_year
            )::scd_type
        ]) records
from this_year_data ty
left join last_year_scd ly on ty.actor_id = ly.actor_id
where (ly.quality_class <> ty.quality_class
or ly.is_active <> ty.is_active)
),  
unnested_changed_records as 
(select 
    actor_id,
	actor_name,
    (records::scd_type).quality_class,
    (records::scd_type).is_active,
    (records::scd_type).start_year,
    (records::scd_type).end_year
from changed_records
)
select *, (select this_year from vars) as creation_year
from historical_scd
union all 
select *, (select this_year from vars) as creation_year
from unchanged_records
union all 
select *, (select this_year from vars) as creation_year
from unnested_changed_records
union all 
select *, (select this_year from vars) as creation_year
from new_records
;