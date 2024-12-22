import os
import uuid
from conf.spark_session import spark
from pyspark.sql.functions import (
    col,
    to_json,
    struct,
    from_json,
    avg,
    current_timestamp,
    lit,
)
from conf.conf import config
from conf.constants import GREEN, END
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
)
from kafka_admin import (
    create_topic,
    kafka_topics,
    kafka_config,
)

# package necesserary for the Kafka reading from Spark Streaming
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars mysql-connector-j-8.0.32.jar "
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 "
    "pyspark-shell"
)

jdbc_url = config.JDBC_URL
jdbc_user = config.JDBC_USER
jdbc_password = config.JDBC_PASSWORD
jdbc_table_results = "athlete_event_results"
jdbc_table_bio = "athlete_bio"

# ---------------------------------------
# Step 1: Read data from athlete_event_bio table and filter them
# ---------------------------------------

# read data from database (athlete_event_bio table)
df_bio = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=jdbc_table_bio,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

df_bio.show(10)
print("Number of rows in athlete_bio table: ", df_bio.count())

# ---------------------------------------
# Step 2: Filter obtained data from athlete_bio table
# ---------------------------------------

df_bio_filtered = df_bio.filter(
    col("height").cast("int").isNotNull() & col("weight").cast("int").isNotNull()
)
print("Filtered number of rows in athlete_bio table: ", df_bio_filtered.count())

df_bio_filtered.show(10)

# ---------------------------------------
# Step 3.2: Read data from Kafka topic and convert them in dataframe
# ---------------------------------------

topic_athlete_event_results = kafka_topics["topic_athlete_event_results"]

# define configurations for Kafka stream data reading
results_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["jaas_config"])
    .option("subscribe", topic_athlete_event_results)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "20")
    .load()
)

# define JSON schema for data reading
json_schema = StructType(
    [
        StructField("country_noc", StringType(), True),
        StructField("athlete_id", DoubleType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
    ]
)

# data manipulations
clean_df = (
    results_df.selectExpr(
        "CAST(key AS STRING) AS key_deserialized",
        "CAST(value AS STRING) AS value_deserialized",
        "*",
    )
    .drop("key", "value")
    .withColumnRenamed("key_deserialized", "key")
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
    .withColumn("country_noc", col("value_json.country_noc"))
    .withColumn("athlete_id", col("value_json.athlete_id"))
    .withColumn("sport", col("value_json.sport"))
    .withColumn("medal", col("value_json.medal"))
    .drop("value_json", "value_deserialized")
)

# ---------------------------------------
# Step 4-5: Join data from Kafka topic with data from database
# ---------------------------------------

processed_df = (
    clean_df.join(df_bio_filtered, ["athlete_id"], "left")
    .select("sport", "medal", "sex", df_bio_filtered["country_noc"], "height", "weight")
    .groupBy("sport", "medal", "sex", "country_noc")
    .agg(avg(col("height")).alias("avg_height"), avg(col("weight")).alias("avg_weight"))
    .withColumn("timestamp", current_timestamp())
    .filter(
        col("country_noc").isNotNull()
        & col("sex").isNotNull()
        & col("sport").isNotNull()
    )
)

# ---------------------------------------
# Step 6: Make a microbatch datastream to Kafka and to MySQL database
# ---------------------------------------

topic_athlete_enriched = kafka_topics["topic_athlete_enriched"]

create_topic(topic_athlete_enriched)

jdbc_table_enriched = "athlete_enriched"
jdbc_url_enriched = (
    f"jdbc:mysql://{config.MYSQL_HOST}:{config.MYSQL_PORT}/oleksandra_tk"
)

# function for each batch processing
def foreach_batch_function(batch_df, batch_id):
    print(f"{GREEN}Processing batch {batch_id}{END}")

    prepared_to_kafka_df = batch_df.withColumn(
        "value", to_json(struct(*batch_df.columns))
    ).withColumn("key", lit(str(uuid.uuid4())))

    # write data to kafka
    prepared_to_kafka_df.write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0]
    ).option("kafka.security.protocol", kafka_config["security_protocol"]).option(
        "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
    ).option(
        "kafka.sasl.jaas.config", kafka_config["jaas_config"]
    ).option(
        "topic", topic_athlete_enriched
    ).save()

    # write data to database
    batch_df.write.format("jdbc").option("url", jdbc_url_enriched).option(
        "driver", "com.mysql.cj.jdbc.Driver"
    ).option("dbtable", jdbc_table_enriched).option("user", jdbc_user).option(
        "password", jdbc_password
    ).mode(
        "append"
    ).save()


# set data stream to process each batch of data
processed_df.writeStream.foreachBatch(foreach_batch_function).outputMode(
    "update"
).start().awaitTermination()
