### Get started with the lab

1. Spin up the containers from set up [README](../../setup_spark/README.md)

    ```bash
    cd ~/git_repos/dataexpert_course_material/setup_spark

    make up
    ```

2. Go to PGadmin UI: [http://localhost:8888](http://localhost:8888) 

3. When done working to spin down all the containers: `make down`

### Working with Spark in jupyter 

* creating a session and importing some data 

    ```{python}
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import expr, col, lit

    # need to create a spark session and work within the app space
    spark = SparkSession.builder.appName("Jupyter").getOrCreate()

    events = spark.read.option("header", "true") \
                    .csv("/home/iceberg/data/events.csv") \
                    .withColumn("event_date", expr("DATE_TRUNC('day', event_time)"))

    events.collect()
    ```

    - `.getOrCreate()` using more camel syntax as opposed to pythonic/snake syntax bc spark is JVM, so using a python wrapper, so implementing a bit of java style in func names 

    - added `event_date` col bc need it for Iceberg

    - `df.collect()` is when executors start working in stages, be wary of this command for if the session is working with a lot of data it can lead to an out of memory error 

    - when needing to render data, make sure to use functions like `.show()` or `.take()`. Avoid using `.collect()` unless really sure that work with very aggregated data 

* repartition data

    ```{python}
    sorted = df.repartition(10, col("event_date"))\
        .sortWithinPartitions(col("event_date"), col("host"))\
        .withColumn("event_time", col("event_time").cast("timestamp")) 

    sortedTwo = df.repartition(10, col("event_date"))\
        .sort(col("event_date"), col("host"))\
        .withColumn("event_time", col("event_time").cast("timestamp")) 

    sorted.show()
    sortedTwo.show()
    ```

    - `.repartition()`: repartitions the data according to a specific column 

    - `.sortWithinPartitions()` vs `.sort()`: the former sorts data within each partition while the latter sorts data globally across partitions. The efficencie of the two are indestinguishable with small data volume, but `.sortWithinPartitions()` is much more performative at scale.

        + with a global sort, required to pass all data through a single executor

* some good to know syntax:

    - `df.join(df, lit(1) == lit(1))`: dirty trick to make your data explode via a cross join

    - `.take(5)`: take the first x records 

    - `.explain()`: prints out the execution plan for the relative series of functions 

        + `Project` is the samething as `Select`

        + `Exchange` is the `.repartition()` function, its essentially the equivalent of shuffle 

            - rangepartitioning --> not very scalable 

            - hashpartitioning --> much better at scale

        + `Sort`: when there is a false then its a sort within the partition, when true its a global sort 

        + `Project` is the final column select 

* creating tbls in Iceberg

    ```{sql}
    %%sql
    CREATE DATABASE IF NOT EXISTS bootcamp
    %%sql
    DROP TABLE IF EXISTS bootcamp.events
    %%sql
    DROP TABLE IF EXISTS bootcamp.events_sorted
    ```

    - `%%sql` decorator to execute SQL within a jupyter notebook cell 

    - best to execute each query within its own cell 

* partitioning Iceberg tbls

    ```{python}
    %%sql
    CREATE TABLE IF NOT EXISTS bootcamp.events (
        url STRING,
        referrer STRING,
        browser_family STRING,
        os_family STRING,
        device_family STRING,
        host STRING,
        event_time TIMESTAMP,
        event_date DATE
    )
    USING iceberg
    PARTITIONED BY (years(event_date));
    ```

    - a tbl can be partition by multiple time dimensions (yrs, days, months etc)

* writting data from python sprak session to Iceberg

    ```{python}
    start_df = df.repartition(4, col("event_date")).withColumn("event_time", col("event_time")).cast("timestamp")
        
    first_sort_df = start_df.sortWithinPartitions(col("event_date"), col("browser_family"), col("host"))

    start_df.write.mode("overwrite").saveAsTable("bootcamp.events_unsorted")
    first_sort_df.write.mode("overwrite").saveAsTable("bootcamp.events_sorted")
    ```

* to look at the sizes of data sets within Iceberg

    ```{python}
    %%sql
    SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
    FROM demo.bootcamp.events_sorted.files

    UNION ALL
    SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
    FROM demo.bootcamp.events_unsorted.files
    ```

    - `.files` is the metadata of the tbl 

    - save more in byte space with sorting due to "run-length-encoding"

        + get better partitioning when have low cardinality dimensions together 

        + ideally should write out data from lowest to highest cardinality 

        + should play around with the sorting cols, there are some for sorting that increase memory efficiency more or less than others