### Get started with the lab

1. Spin up the containers from set up [README](../../setup_postgres/README.md)

    ```bash
    ~/git_repos/dataexpert_intermediate_bootcamp_2025/setup_postgres

    make up
    ```

2. Go to PGadmin UI: [http://localhost:5050](http://localhost:5050) 

3. Go to server from the setup session. If starting from scratch follow setup instructions in [README](../../setup_postgres/README.md)

<details>
    <summary>4. Tbls needed from lab1</summary>

**this approach yields the same results as in lab1, just a different method**

1. create a new data type

    ```sql
    create type season_stats as (
            season integer, 
            gp integer, 
            pts real, 
            reb real,
            ast real
            )
            ;

    create type scoring_class as enum ('star', 'good', 'average', 'bad');
    ```

    + its an enumeration type 

2. create new tbl with additional columns

    ```sql 
    create table players (
        player_name text,
        height text,
        college text, 
        country text,
        draft_year text, 
        draft_round text, 
        draft_number text,
        season_stats season_stats[],
        scoring_class scoring_class, 
        years_since_last_season integer,
        is_active boolean,
        season integer,
    primary key(player_name, season)
    )
    ;
    ```

    3. populate cumulative tbl 

    ```sql
    insert into players 
    WITH years AS (
        -- created a "skeleton" dimension to replicate today and yesterday mechanism 
        SELECT *
        FROM GENERATE_SERIES(1996, 2022) AS season
    -- with the CTE p and players_and_seaons, create 1 row per player_name/season they had in reported stats
    -- excludes rows where player took time off (for example Michael Jordan)
    ), p AS (
        SELECT
            player_name,
            MIN(season) AS first_season
        FROM player_seasons
        GROUP BY player_name
    ), players_and_seasons AS (
        SELECT *
        FROM p
        JOIN years y
            ON p.first_season <= y.season
    ), windowed AS (
        -- same mechanism as today and yesterday CTE join except less manual 
        -- array_remove() is an improvement bc instead of a case condition to keep nulls out of the array, this custom function does 
        SELECT
            pas.player_name,
            pas.season,
            ARRAY_REMOVE(
                ARRAY_AGG(
                    CASE
                        WHEN ps.season IS NOT NULL
                            THEN ROW(
                                ps.season,
                                ps.gp,
                                ps.pts,
                                ps.reb,
                                ps.ast
                            )::season_stats
                    END)
                -- not sure why this colaesce was done this way, would think it should be coalesce(ps.season, pas.seasons)
                OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)), 
                NULL
            ) AS seasons
        FROM players_and_seasons pas
        LEFT JOIN player_seasons ps
            ON pas.player_name = ps.player_name
            AND pas.season = ps.season
        -- order is established here since this is the CTE that builds the array
        ORDER BY pas.player_name, pas.season
    ), static AS (
        -- same as effect as doing the coalesce(today, yesterday) per dim 
        -- this is depedent on that each player will only have 1 unique value per dim 
        SELECT
            player_name,
            MAX(height) AS height,
            MAX(college) AS college,
            MAX(country) AS country,
            MAX(draft_year) AS draft_year,
            MAX(draft_round) AS draft_round,
            MAX(draft_number) AS draft_number
        FROM player_seasons
        GROUP BY player_name
    )
    SELECT
        w.player_name,
        s.height,
        s.college,
        s.country,
        s.draft_year,
        s.draft_round,
        s.draft_number,
        seasons AS season_stats,
        -- this grading is done on how the player performed in the season of question 
        -- in the end, will show how the player was graded throughout their career --> player can have more than 1 unique score in the tbl 
        CASE
            WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
            WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
            WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
            ELSE 'bad'
        END::scoring_class AS scoring_class,
        w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
        (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active,
        w.season
    FROM windowed w
    JOIN static s
        ON w.player_name = s.player_name;
    ```

    + additional notes: 

        - the method in lab1 looks like its a good incremental implementation 

        - this method here looks like it would be good for a backfill 

</details>

<details open>
    <summary>Create an SCD2 table</summary>

1. create the player scd table 

    ```sql 
    create table player_scd (
        player_name text,
        scoring_class scoring_class,
        is_active boolean,
        start_season integer,
        end_season integer,
        current_season integer,
        primary key(player_name, start_season, current_season)
    )
    ;
    ```

    + additonal notes:

        - in the video stated that the primary key is just player_name and start_season, but found that current_season should be in there as well. 

        - the reason for this is in prod, the incremental load will always create a new entry a hard coded current_season value, so a given player_name/scoring_class/is_active/start_season/end_season combo will be associated to more than 1x current_season 

        - with this approach, it also assumes that downstream stakeholders will only query lines where current_season is closest to their time stamp aka where trunc(current_timestamp, 'year') = current_season

2. option A: insert records into the scd tbl as full refresh

    ```sql 
    insert into player_scd 
    with with_previous as 
    -- determines what was the previous row/season status for a given player 
    (select 
        player_name, 
        season current_season,
        scoring_class, 
        is_active,
        lag(scoring_class, 1) over (partition by player_name order by season) previous_scoring_class,
        lag(is_active, 1) over (partition by player_name order by season) previous_is_active
    from players
    where season <= 2021
    ),
    with_indicators as
    -- develop indicator to determine if a change took place
    (select 
        *,
        -- combined the indicator into a single dim to make it easier to track metadata change over time 
        case
            when scoring_class <> previous_scoring_class then 1 
            when is_active <> previous_is_active then 1 
        else 0
        end change_indicator
    from with_previous
    ),
    with_streaks as 
    -- this CTE is meant to calculate how many times scoring_class/is_active dimensions change from season to season
    (select 
        *, 
        sum(change_indicator) over (partition by player_name order by current_season) streak_indicator 
    from with_indicators
    ) 
    -- the group by is meant to collapse all those rows to singular change rows
    select 
        player_name,
        scoring_class,
        is_active,
        min(current_season) first_season,
        max(current_season) last_season,
        2021 current_season
    from with_streaks
    group by player_name,
        streak_indicator,
        scoring_class,
        is_active
    order by player_name,
        first_season
    ```

    + additional info: the goal of this table is to track how long a given player was in a given dimension, aka how long a given player had a is_active/scoring_class combo for consecutive seasons

    + pitfalls

        - query is expensive to run because its using aggregate window and then group by functions 

        - the full refresh query (the one right above) is prone to out of memory issues and data skew

        - in instances where entities experience a lot of change and therefor requires more records dedicated to them --> reduces cardinality of tbl 

        - dimensional data is not as large of volume so more flexible in doing a full refresh of the data 

3. option B: incrementally load the data 

    ```sql 
    create type scd_type as (
    scoring_class scoring_class, 
    is_active boolean,
    start_season integer,
    end_season integer
    );

    insert into player_scd
    with last_season_scd as 
    (select * 
    from player_scd
    where current_season = 2021
    and end_season = 2021
    ),
    historical_scd as 
    -- identify records that wont change anymore bc their time window is "closed"
    (select 
        player_name, 
        scoring_class,
        is_active,
        start_season,
        end_season
    from player_scd
    where current_season = 2021
    and end_season < 2021
    ),
    this_season_data as 
    (select * 
    from players
    where season = 2022
    ),
    unchanged_records as
    -- to ID records that remain the same in the incremental load 
    (select 
        ts.player_name, 
        ts.scoring_class, 
        ts.is_active,
        ls.start_season,
        -- this is to update the latest record of the player/scoring_class/is_active combo to the latest cumulative run "flag" aka current_season
        ts.season end_season 
    from this_season_data ts 
    join last_season_scd ls on ts.player_name = ls.player_name 
    where ls.scoring_class = ts.scoring_class
    and ls.is_active = ts.is_active 
    ),
    new_records as 
    -- ID new player_name that have been found in the player tbl 
    (select 
        ts.player_name, 
        ts.scoring_class, 
        ts.is_active,
        ts.season start_season,
        ts.season end_season 
    from this_season_data ts 
    left join last_season_scd ls on ts.player_name = ls.player_name 
    where ls.player_name is null 
    ), 
    changed_records as 
    (select 
        ts.player_name, 
        unnest(array[
                -- this is to carry over the old player_name/scoring_class/is_active combo 
                -- may have the exact combo in the tbl already but with different current_season
                row(ls.scoring_class, 
                ls.is_active,
                ls.start_season,
                ls.end_season
                )::scd_type,
                -- this is to insert into the scd tbl new combos found in players
                row(ts.scoring_class, 
                ts.is_active,
                ts.season,
                ts.season
                )::scd_type
            ]) records
    from this_season_data ts 
    left join last_season_scd ls on ts.player_name = ls.player_name 
    where (ls.scoring_class <> ts.scoring_class
    or ls.is_active <> ts.is_active)
    ),
    unnested_changed_records as 
    (select 
        player_name,
        (records::scd_type).scoring_class,
        (records::scd_type).is_active,
        (records::scd_type).start_season,
        (records::scd_type).end_season
    from changed_records
    )
    select *, 2022 current_season
    from historical_scd
    union all 
    select *, 2022 current_season
    from unchanged_records
    union all 
    select *, 2022 current_season
    from unnested_changed_records
    union all 
    select *, 2022 current_season
    from new_records
    ;
    ```

    + additonal notes - load method:

        - the incremental load will always create a new entry a hard coded current_season value, so a given player_name/scoring_class/is_active/start_season/end_season combo will be associated to more than 1x current_season 

        - with this approach, it also assumes that downstream stakeholders will only query lines where current_season is closest to their time stamp aka where trunc(current_timestamp, 'year') = current_season

    + additonal notes - caveats/benefits:

        - the current approach doesnt account for situations where is_active is null or scoring_class is null. this can cause issues in change_records CTE, where thos null rows are accidently filtered out. --> best to build in exceptions to account for this 

        - the benefit of the incremental load is that is querying a lower volume of data, only previous year as a select and current year for the transformation part 

        - this is sequential data though, so makes its a bit trickier to back fill --> prob in that case have to resort to full refresh 



</details>