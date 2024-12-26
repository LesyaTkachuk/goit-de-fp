from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

# create spark session
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()


def main():
    # load dataset
    df_bio = spark.read.parquet("task_2/silver/athlete_bio", header=True)
    df_results = spark.read.parquet("task_2/silver/athlete_event_results", header=True)

    # join dataframes
    df_joined = (
        df_bio.join(df_results, ["country_noc", "athlete_id"], "inner")
        .groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            avg(col("height")).alias("avg_height"),
            avg(col("weight")).alias("avg_weight"),
        )
        .withColumn("timestamp", current_timestamp())
    )

    df_joined.show(10)
    df_joined.write.format("parquet").mode("overwrite").save("task_2/gold/avg_stats")


if __name__ == "__main__":
    main()
