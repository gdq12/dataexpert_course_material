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
    actor_name text not null,
    films films[],
    quality_class quality_class not null default 'unknown', 
    is_active boolean not null,
    creation_year integer not null,
    primary key(actor_id, creation_year)
)
;

create index idx_actor_year on actors(actor_id, creation_year);

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
group by actorid,
        actor,
        year
),
nested as 
(select 
	coalesce(t.actorid, y.actor_id) as actor_id,
	coalesce(t.actor, y.actor_name) as actor_name, 
    -- since this is a cumulative table will cumulate all movies the actor has worked on to date 
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
	primary key(actor_id, start_year, end_year)
)
;

create index idx_actor_start_end on actors(actor_id, start_year, end_year);

-- 4.Backfill query for actors_history_scd
insert into actors_history_scd
with vars as 
(select 2009 as target_year
)
select 
    actor_id,
	actor_name, 
	quality_class,
	is_active,
	min(creation_year) as start_year,
    max(creation_year) as end_year
from actors
where creation_year <= (select target_year from vars)
group by actor_id,
	actor_name, 
	quality_class,
	is_active
order by actor_id,
		start_year
;

-- 5.Incremental query for actors_history_scd
insert into actors_history_scd
with latest_year as 
(select 
	max(end_year) end_year
from actors_history_scd
)
select 
    actor_id,
	actor_name, 
	quality_class,
	is_active,
	min(creation_year) as start_year,
    max(creation_year) as end_year
from actors
where creation_year > (select end_year from latest_year)
group by actor_id,
	actor_name, 
	quality_class,
	is_active
;