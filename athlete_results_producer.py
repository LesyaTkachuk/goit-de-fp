import uuid
import time
from conf.spark_session import spark
from conf.conf import config
from pyspark.sql.functions import col, to_json, struct, from_json, from_unixtime, lit
from kafka_admin import (
    create_topic,
    create_producer,
    create_consumer,
    kafka_topics,
    kafka_config,
)
from conf.constants import GREEN, RED, END

# ---------------------------------------
# Step 3.1: Read data from athlete_event_results table and push them to Kafka topic
# ---------------------------------------

jdbc_url = config.JDBC_URL
jdbc_user = config.JDBC_USER
jdbc_password = config.JDBC_PASSWORD
jdbc_table_results = "athlete_event_results"
jdbc_table_bio = "athlete_bio"

df_results = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=jdbc_table_results,
        user=jdbc_user,
        password=jdbc_password,
    )
    .load()
)

df_results.show(10)


topic_athlete_event_results = kafka_topics["topic_athlete_event_results"]

create_topic(topic_athlete_event_results)

prepared_df = df_results.select("country_noc", "athlete_id", "sport", "medal")

prepared_df.show(10)

try:
    athlete_results_producer = create_producer()
    print(f"{GREEN}Created producer{END}")

    # push data to Kafka
    for row in prepared_df.collect():
        athlete_results_producer.send(
            topic_athlete_event_results,
            key=str(uuid.uuid4()),
            value=row.asDict(),
        )

    athlete_results_producer.flush()

    print(
        f"{GREEN}Successfully sent athlete results data to topic {topic_athlete_event_results}{END}"
    )
    athlete_results_producer.close()

except KeyboardInterrupt:
    print(f"{RED}Keyboard interrupt received. Exiting...{END}")
except Exception as e:
    print(
        f"{RED}Error sending athlete results data to topic {topic_athlete_event_results}: {e}{END}"
    )
