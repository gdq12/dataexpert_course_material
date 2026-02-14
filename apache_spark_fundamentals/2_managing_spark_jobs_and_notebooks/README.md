### Get started with the lab

1. Spin up the containers from set up [README](../../setup_spark/README.md)

    ```bash
    cd ~/git_repos/dataexpert_course_material/setup_spark

    make up
    ```

2. Go to PGadmin UI: [http://localhost:8888](http://localhost:8888) 

3. this lesson is based on scala so use the `spylon-kernel` when working with the notebooks.

4. When done working to spin down all the containers: `make down`

5. for further setup configs or debugging checkout the [setup_spark/README.md](../../setup_spark/README.md).

### Exploring Dataset API

**This is the API that works in scala only**

*using the [DatasetApi.ipynb](../../setup_spark/notebooks/DatasetApi.ipynb) notbook here*

1. open a spark session and imprt all needed libs 

    ```{scala}
    //start a spark session 
    import org.apache.spark.sql.SparkSession 
    import org.apache.spark.sql.Dataset
    import sparkSession.implicits._ 

    val sparkSession = SparkSession.builder.appName("Juptyer").getOrCreate()
    ```

2. establishing Dataset schema

    ```{scala}
    // defining a schema 
    case class Event (
    //Option is a way to handle NULL more gracefully
        user_id: Option[Integer],
        device_id: Option[Integer],
        referrer: Option[String],
        host: String,
        url: String,
        event_time: String
    )

    case class EventWithDeviceInfo (
    user_id: Integer,
        device_id: Integer,
        browser_type: String,
        os_type: String,
        device_type: String,
        referrer: String,
        host: String,
        url: String,
        event_time: String
    )
    ```

    * wrapping up data type in `Option[]`:

        + this is necessary when needing to serialize data and the dimension is nullable.

        + wrapping is not needed for dimensions that aren't nullable.

        + **what is the benefit of this?**: by enforcing nullability into the schema, it presumes that those dimensions that aren't wrapped should never be null. In the chance that records inserted into the schema don't comply with this assumption, will raise an error. This is a good thing to alert AE/DE of the data quality error early on.

        + enforce assumptions of data (Nullability/non-nullability) at runtime.

    * creating dummy data to test out and enforce data quality: can create a dummy set data with both all valid or intended faulty value to see how the API will import data when comparing to the schema. Does the faulty data import well with the schema? If yes then the schema needs to be updated.


3. creating datasets 

    ```{scala}
    val events: Dataset[Event] = sparkSession.read.option("header", "true")
                            .option("inferSchema", "true")
                            .csv("/home/iceberg/data/events.csv")
                            .as[Event]

    val devices: Dataset[Device] = sparkSession.read.option("header", "true")
                            .option("inferSchema", "true")
                            .csv("/home/iceberg/data/devices.csv")
                            .as[Device]

    devices.createOrReplaceTempView("devices")
    events.createOrReplaceTempView("events")

    ```

    * `.as[className]`: turns the imported data into a dataset. Make sure that the dataset schema is defined before hand.

    * `: Dataset[Event]`: defining the data type of the object. This is infered in scala so not mandatory but good practice to implement.

4. working with the datasets in scala 

    ```
    val filteredViaDataset = events.filter(event => event.user_id.isDefined && event.device_id.isDefined)
    val filteredViaDataFrame = events.toDF().where($"user_id".isNotNull && $"device_id".isNotNull)
    val filteredViaSparkSql = sparkSession.sql("SELECT * FROM events WHERE user_id IS NOT NULL AND device_id IS NOT NULL")
    ```

    * `events.filter(event => datasetName.dimName.isDefined && datasetName.dimName.isDefined)`: this looks for records where the target dims (`datasetName.dimName`) are present, aka no null.

    * the 3 definitions of new datasets here transform the data for the same end result, aka `.isDefined` == `.isNotNull` == `select...`

### Joins with datasets 

1. SparkSQL method 

    ```{scala}
    //Creating temp views is a good strategy if you're leveraging SparkSQL
    filteredViaSparkSql.createOrReplaceTempView("filtered_events")
    val combinedViaSparkSQL = spark.sql(f"""
        SELECT 
            fe.user_id,
            d.device_id,
            d.browser_type,
            d.os_type,
            d.device_type,
            fe. referrer,
            fe.host,
            fe.url,
            fe.event_time
        FROM filtered_events fe 
        JOIN devices d ON fe.device_id = d.device_id
    """)
    ```

    * most simple method, its using syntax that everyone is familiar with 

2. Dataframe method

    ```{scala}
    // defining UDF for dataframe
    def toUpperCase(s: String): String {
        return s.toUpperCase()
    }
    val toUpperCaseUDF = udf(toUpperCase _) // the '_' is so the function can be passed as a value 

    // DataFrames give up some of the intellisense because you no longer have static typing
    val combinedViaDataFrames = filteredViaDataFrame.as("e")
                //Make sure to use triple equals when using data frames
                .join(devices.as("d"), $"e.device_id" === $"d.device_id", "inner")
                .select(
                $"e.user_id",
                $"d.device_id",
                toUpperCaseUDF($"d.browser_type").as("browser_type"),
                $"d.os_type",
                $"d.device_type",
                $"e.referrer",
                $"e.host",
                $"e.url",
                $"e.event_time"
                )
    ```

    * `$"e.user_id"` is equivalent to `col("e.user_id")`

    * advantages: this method allows abstraction of the different steps. Built-in or custom functions can be implemented as a step via `.funcName()`. This leads to ability to break up join job a bit for better customization.

    * `toUpperCaseUDF($"d.browser_type").as("browser_type")`: passing a UDF through this type of API

3. Dataset method 

    ```{scala}
    // creating a simple UDF
    def toUpperCase(s: String): String {
        return s.toUpperCase()
    }

    // This will fail if user_id is None
    val combinedViaDatasets = filteredViaDataset
        .joinWith(devices, events("device_id") === devices("device_id"), "inner")
        .map{ case (event: Event, device: Device) => EventWithDeviceInfo(
                    user_id=event.user_id.get,
                    device_id=device.device_id,
                    browser_type=device.browser_type,
                    os_type=device.os_type,
                    device_type=device.device_type,
                    referrer=event.referrer.getOrElse("unknow"),
                    host=event.host,
                    url=event.url,
                    event_time=event.event_time
                ) }
    .map { eventWithDevice =>
            // Convert browser_type to uppercase while maintaining immutability
            eventWithDevice.copy(browser_type = eventWithDevice.browser_type.toUpperCase)
        }
    ```

    * in this method need to implement on how to deal with nulls

    * easier to maps data with this method bc when joining the 2 Datasets auto have access to the schemas, so --> better mapping of data btw sets during the join.

    * `.get`: this presume that the data set that its working with already has the nulls fitered out.

    * `.getOrElse()`: this is the equivalent of coalesce

    * permits further data transformation with pure scala syntax: defining a UDF and mapping the transformation to the joined Dataset

### Caching 

*using the [Caching.ipynb](../../setup_spark/notebooks/Caching.ipynb) notbook here*

* Background on the datasets:

    - using [events](../../setup_spark/data/events.csv) and [devices](../../setup_spark/data/devices.csv) datasets here 

    - there is a many to many relationship between the 2 tables, users can have many devices and a device can be linked to more than 1 user.

    - the goal here is to create new datasets that encapsulates a user with all of their devices **AND** a devices with all of the users its linked to.

1. setup session and configs 

    ```{scala}
    import org.apache.spark.sql.functions._
    import org.apache.spark.storage.StorageLevel

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "4")
    ```

2. setup dataset schema and import data 

    ```{scala}
    val users = spark.read.option("header", "true")
                            .option("inferSchema", "true")
                            .csv("/home/iceberg/data/events.csv")
                            .where($"user_id".isNotNull)

    users.createOrReplaceTempView("events")

    val devices = spark.read.option("header", "true")
                            .option("inferSchema", "true")
                            .csv("/home/iceberg/data/devices.csv")

    devices.createOrReplaceTempView("devices")
    ```

3. perform aggregations to "create new datasets that encapsulates a user with all of their devices **AND** a devices with all of the users its linked to."

    ```{scala}
    val executionDate = "2023-01-01"

    val eventsAggregated = spark.sql(f"""
    SELECT user_id, 
            device_id, 
            COUNT(1) as event_counts, 
            COLLECT_LIST(DISTINCT host) as host_array
    FROM events
    GROUP BY 1,2
    """).cache()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bootcamp.events_aggregated_staging (
            user_id BIGINT,
            device_id BIGINT,
            event_counts BIGINT,
            host_array ARRAY<STRING>
        )
        PARTITIONED BY (ds STRING)
    """)

    val usersAndDevices = users
    .join(eventsAggregated, eventsAggregated("user_id") === users("user_id"))
    .groupBy(users("user_id"))
    .agg(
        users("user_id"),
        max(eventsAggregated("event_counts")).as("total_hits"),
        collect_list(eventsAggregated("device_id")).as("devices")
    )

    val devicesOnEvents = devices
        .join(eventsAggregated, devices("device_id") === eventsAggregated("device_id"))
        .groupBy(devices("device_id"), devices("device_type"))
        .agg(
            devices("device_id"),
            devices("device_type"),
            collect_list(eventsAggregated("user_id")).as("users")
        )
    ```

    * High level overview of transformation here:

        - using the event data to calc the num of events and list of hist per user and devide id combo --> `eventsAggregated` --> **this allows then to calc down stream stats for a given user and device id**

        - finding the max num events and list of linked devices for a given user from `eventsAggregated` --> `usersAndDevices`

        - finding the list of linked users per device id and type from `eventsAggregated` --> `devicesOnEvents`

4. implications of Caching here 

    ```{scala}
    // to be ableto deep dive into the transformation plans 

    devicesOnEvents.explain()
    usersAndDevices.explain()

    devicesOnEvents.take(1)
    usersAndDevices.take(1)
    ```

    * the transformation code from pt.3 was run 2x: `eventsAggregated` w/o cache and then with `.cache()`

    * highlights of the difference for the plans observed via a text diff:

        - the non-cache run is performing the aggregations (`HashAggregate`) while the cache version is not re-importing data and performing aggregations but instead reading from an in-memory table (`InMemoryTableScan`).

        - in the `.cache()` query plan it may appear that there are more steps, but that just includes the reading from memory 

        - only benefit from cahce when executing queries more than 1x, when reusing tbls 

    * cahce configs 

        - `.cahce()`: the default here is `StorageLevel.MEMORY_ONLY`

        - `.persists()`: can use this to write data to disk (`StorageLevel.DISK_ONLY`), but this increases network I/O so better alternative is to export the data  

        - `.cahce()` == `.persists(StorageLevel.MEMORY_ONLY)`
        
        - `eventsAggregated.write.mode("overwrite").saveAsTable("bootcamp.events_aggregated_staging")`: better alternative to `StorageLevel.DISK_ONLY`. Allows for insight into the intermdiate steps of the job, make it easier for future investigation.

### Bucket joins in Iceberg

*using the [bucket-joins-in-iceberg.ipynb](../../setup_spark/notebooks/bucket-joins-in-iceberg.ipynb) notbook here*

1. start spark session and import data 

    ```{scala}
    import org.apache.spark.sql.functions.{broadcast, split, lit}

    val matchesBucketed = spark.read.option("header", "true")
                            .option("inferSchema", "true")
                            .csv("/home/iceberg/data/matches.csv")
    val matchDetailsBucketed =  spark.read.option("header", "true")
                            .option("inferSchema", "true")
                            .csv("/home/iceberg/data/match_details.csv")
    ```

2. creating Iceberg schemas

    ```{scala}
    spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")
    val bucketedDDL = """
    CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
        match_id STRING,
        is_team_game BOOLEAN,
        playlist_id STRING,
        completion_date TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (completion_date, bucket(16, match_id));
    """
    spark.sql(bucketedDDL)

    val bucketedDetailsDDL = """
    CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
        match_id STRING,
        player_gamertag STRING,
        player_total_kills INTEGER,
        player_total_deaths INTEGER
    )
    USING iceberg
    PARTITIONED BY (bucket(16, match_id));
    """
    spark.sql(bucketedDetailsDDL)
    ```

    * partitioning the schemas by buckets based ondate (where applicable) and buckets of `match_id`.

3. write out data to Iceberg

    ```{scala}
    matchesBucketed.select(
        $"match_id", $"is_team_game", $"playlist_id", $"completion_date"
        )
        .write.mode("append")
        .partitionBy("completion_date")
        .bucketBy(16, "match_id").saveAsTable("bootcamp.matches_bucketed")

    matchDetailsBucketed.select(
        $"match_id", $"player_gamertag", $"player_total_kills", $"player_total_deaths"
        )
        .write.mode("append") // overwrite doesnt work here for some reason
        .bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed")
    ```

    * the `match_id` are distributed across 16 buckets in Iceberg

4. query the Iceberg tables to examine the bucket effect 

    ```{python}
    %%sql 
    SELECT * FROM bootcamp.matches_bucketed.files

    %%sql
    SELECT * FROM bootcamp.matches_details_bucketed.files
    ```

    * when examining the query results, will see that in `bootcamp.matches_details_bucketed.files` the table is partitioned just the bucketed match_id. In `bootcamp.matches_bucketed.files` the table is paritioned by not only the bucketed match_id but also the completion date.

5. comparing broadcast join performance in Iceberg vs imported files 

    ```{scala}
    // the -1 parameter disables broadcast joins 
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    matchesBucketed.createOrReplaceTempView("matches")
    matchDetailsBucketed.createOrReplaceTempView("match_details")

    // this is the one that queries the Iceberg
    spark.sql("""
        SELECT * FROM bootcamp.match_details_bucketed mdb JOIN bootcamp.matches_bucketed md 
        ON mdb.match_id = md.match_id
        AND md.completion_date = DATE('2016-01-01')
            
    """).explain()

    // this is quering the imported file data 
    spark.sql("""
        SELECT * FROM match_details mdb JOIN matches md ON mdb.match_id = md.match_id
            
    """).explain()
    ```

    * bucket joins are really only needed when working with very large data 

    * `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`: by default broadcast joins are used, the "-1" is implemented to disable this and instead use bucket joins 

    * comparison of plans:

        - the query plan using Iceberg bucketed tables is much more compact compared to the imported data one 

        - for the imported data, there is a lot more data exchange (`Exchange hashpartition`) --> demonstrates a lot of shuffling will occur

        - query plan for the Iceberg bucket tables show no shuffling will occur. Still does the sort-merge-join but in the bucket, not global or partitioning.

        - the bucket query plan: `BatchScan` == bucket scan --> `SortMergeJoin`.

        - at scale, using bucketing before joining dramatically increases efficiency.