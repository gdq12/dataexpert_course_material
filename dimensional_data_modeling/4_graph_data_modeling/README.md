### Get started with the lab

1. Spin up the containers from set up [README](../../setup_postgres/README.md)

    ```bash
    ~/git_repos/dataexpert_intermediate_bootcamp_2025/setup_postgres

    make up
    ```

2. Go to PGadmin UI: [http://localhost:5050](http://localhost:5050) 

3. Go to server from the setup session. If starting from scratch follow setup instructions in [README](../1_setup_postgres/README.md)

<details open>
  <summary>creating tbls</summary>

1. create vertices tbl 

    ```sql
    -- good to know for dropping data types 
    drop type type_name cascade;
    ```

    ```sql
    create type vertex_type as enum (
        'player',
        'team',
        'game'
    )
    ;

    create table vertices (
    identifier text, 
    type vertex_type,
    properties json,
    primary key (identifier, type)
    )
    ;

    create type edge_type as enum (
        'plays_against',
        'shares_team',
        'plays_in',
        'plays_on'
    )
    ;

    -- in some DBs will just include edge_id to create a surrogate key but not needed for demo purposes
    create table edges (
        subject_identifier text, 
        subject_type vertex_type,
        object_identifier text,
        object_type vertex_type,
        edge_type edge_type,
        properties json,
        primary key(subject_identifier, 
                    subject_type, 
                    object_identifier, 
                    edge_type)
    )
    ;
    ```

2. populate the vertices tbl 

    ```sql
    insert into vertices  
    select 
        game_id identifier,
        'game'::vertex_type type,
        json_build_object(
                'pts_home', pts_home,
                'pts_away', pts_away,
                'winning_team', case when home_team_wins = 1 then home_team_id else visitor_team_id end
            ) properties
    from games
    ;

    insert into vertices
    with dedup as 
    (select 
        *,
        row_number() over (partition by game_id, player_id order by pts desc) row_num
    from game_details
    ),
    player_agg as 
    (select 
        player_id identifier,
        max(player_name) player_name,
        count(1) number_of_games,
        sum(pts) total_points,
        array_agg(distinct team_id) teams
    from dedup
    group by player_id
    )
    select 
        identifier,
        'player'::vertex_type type, 
        json_build_object(
            'player_name', player_name,
            'number_of_game', number_of_games,
            'total_points', total_points,
            'teams', teams
        ) properties 
    from player_agg 
    ;

    insert into vertices
    with team_dedup as 
    (select  
        *,
        row_number() over (partition by team_id order by nickname) row_num
    from teams
    )
    select
        team_id identifier,
        'team'::vertex_type type,
        json_build_object(
            'abbreviation', abbreviation,
            'nickname', nickname,
            'city', city,
            'arena', arena,
            'year_founded', yearfounded
        ) properties
    from team_dedup
    where row_num = 1
    ;
    ```

    + additional notes: 

        - game_details and teams tbls have duplicates so must dedup

        - 2 methods: distinct or row_number()

        - distinct method: if all dedup rows are exactly the same

        - row_number method: if there is  dim that i different for each dup, then do are order by to decide which row to take from each partition 

    + good to knows:

        - can think of vertices as a mix of SCD and transactional info 

        - game vertices can be thought of as transactional because they really only occur 1x and have their own metadata/metrics associated to that event 

        - the player and team vertices are more like SCD. The player SCD is an aggregation of all the player stats throughout their career to date 

3. populate the edges tbl 

    ```sql 
    -- calc the plays_in edges 
    insert into edges
    with dedup as 
    (select 
        *,
        row_number() over (partition by game_id, player_id order by pts desc) row_num
    from game_details
    )
    select 
        player_id subject_identifier,
        'player'::vertex_type subject_type,
        game_id object_identifier,
        'game'::vertex_type object_type,
        'plays_in'::edge_type edge_type,
        json_build_object(
            'start_position', start_position,
            'pts', pts,
            'team_id', team_id,
            'team_abbreviation', team_abbreviation
        ) properties
    from dedup
    where row_num = 1
    ;

    -- calc the teammates and plays against edges 
    insert into edges
    with dedup as 
    (select 
        *,
        row_number() over (partition by game_id, player_id order by pts desc) row_num
    from game_details
    ),
    filtered as 
    (select * from dedup where row_num = 1
    ),
    aggregated as 
    (select 
        pl1.player_id subject_player_id, 
        pl2.player_id object_player_id,
        -- excluding these calcs cause not even used in the end 
        -- max(pl1.player_name) subject_player_name,
        -- max(pl2.player_name) object_player_name,
        case 
            when pl1.team_abbreviation = pl2.team_abbreviation
                then 'shares_team'::edge_type
            else 'plays_against':: edge_type
        end edge_type,
        count(1) num_games, 
        sum(pl1.pts) subject_points,
        sum(pl2.pts) object_points
    from filtered pl1 
    join filtered pl2 on pl1.game_id = pl2.game_id
                    and pl1.player_id <> pl2.player_id
    where pl1.player_id > pl2.player_id -- to verify there are no double edges, aka where plays with and share team with for a given combo dont appear 2x just due to player name ordered reverse
    group by 1,2,3
    )
    select 
        subject_player_id subject_identifier,
        'player'::vertex_type subject_type, 
        object_player_id object_identifier,
        'player'::vertex_type object_type,
        edge_type,
        json_build_object(
            'num_games', num_games,
            'subject_points', subject_points,
            'object_points', object_points
        )
    from aggregated 
    ;
    ```

    + additional good to knows:

        - edges can be SCD or transactions 
        
        - SCD: storing metrics/metadata from a relationship between 2 entities as an aggregation (rolling up multi events) provides the latest stats for that distinct combo of interaction

        - transaction: samething as SCD rational except the timedate is included in the properties, BUT only if the interaction occured only 1x

        - the limitation of the transaction - edge association is that then it would make the edge non-unique and violate the primary key restriction. Would say then the SCD is a bit more like a transaction is the interaction occured only 1x, but if it occured multiple time then its more like an SCD.

</details>

### Analytics queries 

* what was the most number of pts a player ever scored in a game?

    ```sql
    select 
        v.properties->>'player_name' player_name,
        (v.properties->>'total_points')::integer player_total_points,
        e.edge_type,
        max((e.properties->>'pts')::integer) most_points_scored,
        sum((e.properties->>'pts')::integer) edge_total_points
    from vertices v 
    join edges e on v.identifier = e.subject_identifier
                and v.type = e.subject_type
                and e.edge_type = 'plays_in'::edge_type
    group by 1, 2, 3
    order by 1, 2, 4
    ;
    ```

    + additional notes: `player_total_points` and `edge_total_points` was just a spot check to verify that aggregations were performed correctly btw vertices and edges 

* what is a players scoring rate when playing either with or against a given player compared toe their total career score? 

    ```sql
    select 
        v.properties->>'player_name' subject_player_name,
        v2.properties->>'player_name' object_player_name,
        e.edge_type,
        (e.properties->>'num_games')::integer edge_num_games,
        (v.properties->>'total_points')::integer subject_total_points,
        (v2.properties->>'total_points')::integer object_total_points,
        (e.properties->>'num_games')::real/
        case
            when (v.properties->>'total_points')::integer = 0 
                then 1 
            else (v.properties->>'total_points')::real
        end subject_ball_performance,
        (e.properties->>'num_games')::real/
        case
            when (v2.properties->>'total_points')::integer = 0 
                then 1 
            else (v2.properties->>'total_points')::real
        end object_ball_performance
        
    from vertices v 
    join edges e on v.identifier = e.subject_idnetifier
                and v.type = e.subject_type
    join vertices v2 on v2.identifier = e.object_identifier
                    and v.type = e.object_type 
    where e.object_type = 'player'::vertex_type
    ;
    ```

    + additional notes: 
        
        - would say that the limitation to this analysis is that it is very difficult to implement time-dependent dimensions into the analysis since the concept of graph SQL is more to study relationships as oppposed to just entities
        
        - by default, vertices and edge calcs must be role ups since one can only hace 1 unque entry of each for the respective tables

        - would say best this concept is best to observe stats based on the sum of the lastest data available

        - for basketball stats time only really matters for tracking career progression but for relationship analysis guess not as much 