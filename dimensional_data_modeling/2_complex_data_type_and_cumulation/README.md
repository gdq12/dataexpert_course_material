### Get started with the lab

1. Spin up the containers from set up [README](../../setup_postgres/README.md)

    ```bash
    ~/git_repos/dataexpert_intermediate_bootcamp_2025/setup_postgres

    make up
    ```

2. Go to PGadmin UI: [http://localhost:5050](http://localhost:5050) 

3. Go to server from the setup session. If starting from scratch follow setup instructions in [README](../../setup_postgres/README.md)

<details open>
  <summary>Resolving temporal issue in player_seasons tbl</summary>

* current state of the table is that it has a "temporal issue" --> player_name/season stats are rendered as 1 record

* should this tbl be joined to down stream tables, itll mess with the shuffling and cause decompression 

* solution --> nesting the stats into arrays 

* initial observations of the tbl: 

    + there is a lot of duplicate data 

    + it seems this tbl consists of a mix of dimension and facts/transactions

* steps:

    1. create a data type:

        ```sql
        create type season_stats as (
        season integer, 
        gp integer, 
        pts real, 
        reb real,
        ast real
        )
        ;
        ```
    
    2. create a dim table for player metadata 

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
        current_season integer,
        primary key(player_name, current_season)
        )
        ;
        ```

        + note: age and weight are excluded for now since they can fluctuate by season 

        + adding col current_seasons because this will be a cumulative tbl 

    3. cumulative select query 

        ```sql
        with yesterday as 
        (select *
        from players
        where current_season = 1995
        )
        ```
        + this will usually return all nulls when the tb is first being generated but then will actually add records over time 

        ```sql
        , today as 
        (select * 
        from player_seasons
        where seasons = 1996
        )
        ```
        + this is the seed query for the cumulation table 

        ```sql
        select 
            coalesce(t.player_name, y.player_name) player_name,
            coalesce(t.height, y.height) height,
            coalesce(t.college, y.college) college,
            coalesce(t.country, y.country) country,
            coalesce(t.draft_year, y.draft_year) draft_year,
	        coalesce(t.draft_round, y.draft_round) draft_round,
            coalesce(t.draft_number, y.draft_number) draft_number,
            case 
                when y.season_stats = null 
                    then array[row(
                        t.season, 
                        t.gp, 
                        t.pts,
                        t.reb,
                        t.ast
                    )::season_stats]
                when t.season is not null 
                    then y.season_stats||array[row(
                        t.season, 
                        t.gp, 
                        t.pts,
                        t.reb,
                        t.ast
                    )::season_stats]
                else y.season_stats end season_stats,
            coalesce(t.season, y.current_season + 1) current_season
        from today t 
        full outer join yesterday y on t.player_name = y.player_name 
        ;
        ```

        + colaesce values that are not temporal (aka dimensional values) 

        + season_stats column:

            - when a player previously had no reported stats, the stats from the today CTE are nested into an array

            - when the player has previously reported stats, the today CTE stats are nested into an array and concatenated to array from the yesterday CTE

            - when the player has no reported stats from the today CTE, then the latest stat array is carried over (usually for retired players or those that took a break)

        + current_season column: it either takes the season value from the today CTE, else it'll take the latest season and add 1yr

    4. turn the query into a pipeline 

        + just add a `insert into players` at the top of the select query 

* once the table is cumulative (a bit like a master table) it can be unnested again to the raw form (old schema):

    ```sql 
    with unnested as 
    (select 
        player_name, 
        unnest(season_stats)::season_stats season_stats
    from players 
    )
    select 
        player_name, 
        (season_stats::season_stats).*
    from unnested
    ;
    ```

    + advantages:

        - the player_name grain is kept, aka this can easily be joined to other tables where it is necessary to keep 1 row per player_name/current_season

        - the sorting by player_name and current_season is kept 

    + suggested query to unnest arrays for all player without rendering duplicate records:

        ```sql
        with unnested as 
        (select 
            p.player_name, 
            unnest(p.season_stats) season_stats
        from players p
        join (select player_name, max(current_season) current_season 
            from players
            group by player_name) mx on p.player_name = mx.player_name 
                            and p.current_season = mx.current_season
        )
        select 
            player_name, 
            (season_stats::season_stats).*
        from unnested
        ;
        ```

</details>

<details open>
  <summary>Adding analytical function to cumulative tbl</summary>

1. create a new data type that deals more with scoring/classification 

    ```sql
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
    current_season integer,
    primary key(player_name, current_season)
    )
    ;
    ```

3. select query for insert

    ```sql
    with yesterday as 
    (select *
    from players
    where current_season = 2001
    )
    , today as 
    (select * 
    from player_seasons
    where season = 2002
    )
    select 
        coalesce(t.player_name, y.player_name) player_name,
        coalesce(t.height, y.height) height,
        coalesce(t.college, y.college) college,
        coalesce(t.country, y.country) country,
        coalesce(t.draft_year, y.draft_year) draft_year,
        coalesce(t.draft_round, y.draft_round) draft_round,
        coalesce(t.draft_number, y.draft_number) draft_number,
        case 
            when y.season_stats = null 
                then array[row(
                    t.season, 
                    t.gp, 
                    t.pts,
                    t.reb,
                    t.ast
                )::season_stats]
            when t.season is not null 
                then y.season_stats||array[row(
                    t.season, 
                    t.gp, 
                    t.pts,
                    t.reb,
                    t.ast
                )::season_stats]
            else y.season_stats end season_stats,
        case 
            when t.season is not null 
                case 
                    when t.points > 20 
                        then 'star'
                    when t.points > 15
                        then 'good'
                    when t.points > 10
                        then 'average'
                    else 'bad'
                        end scoring_class::scoring_class
            else y.scoring_class
                end scoring_class,
        case 
            when t.season is not null 
                then 0 
            else y.years_since_last_season + 1 
                end years_since_last_season,
        coalesce(t.season, y.current_season + 1) current_season
    from today t 
    full outer join yesterday y on t.player_name = y.player_name 
    ;
    ```

    + `years_since_last_season`

        - the logic determines for the today CTE year, whether the player_name has reported stats

        - if the player does not have reported stats for the season in today CTE, then it means the player is taking time off or retired and that time duration in yrs is calculated here 

    + `scoring_class`

        - it calculates a scoring class for the player_name if it has reported stats in the today CTE, else it will take the yesterday CTE scoring_class 

        - with the data type `ENUM`, there are a limited of possible values that can be used to populate this dimension 

    + this is an example of building cumulative stats incrementaly 

4. analyze player pts performance from their first reported year to their last 

    ```sql
    select 
        player_name,
        (season_stats[1]::season_stats).pts first_season,
        (season_stats[cardinality(season_stats)]::season_stats).pts last_season,
        (season_stats[cardinality(season_stats)]::season_stats).pts/
            case 
                when (season_stats[1]::season_stats).pts = 0 
                    then 1 
                else (season_stats[1]::season_stats).pts end performance_coefficient
    from players 
    order by performance_coefficient desc 
    ```

    + advantages 

        - this query doesnt require a group by to calc the min and max pts per player

        - the most performing expensive part of the query is the order by cause it requires sorting 

        - incrementaly builds up history 

        - gives access to historical analysis 

        - its more or less paralleizing the computation by removing the need to do a group by 

</details>