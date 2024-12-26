from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

# create spark session
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# function to clean data
def clean_text(text):
    return re.sub(r"[^a-zA-Z0-9,.\"\' ]", "", str(text))

def main():
    # load dataset
    df_bio = spark.read.parquet("task_2/bronze/athlete_bio", header=True)
    df_results = spark.read.parquet("task_2/bronze/athlete_event_results", header=True)

    clean_text_udf = udf(clean_text, StringType())

    # clean string columns
    df_bio = df_bio.withColumn("name", clean_text_udf(df_bio["name"]))
    df_bio = df_bio.withColumn("sex", clean_text_udf(df_bio["sex"]))
    df_bio = df_bio.withColumn("born", clean_text_udf(df_bio["born"]))
    df_bio = df_bio.withColumn("country", clean_text_udf(df_bio["country"]))
    df_bio = df_bio.withColumn("country_noc", clean_text_udf(df_bio["country_noc"]))
    df_bio = df_bio.withColumn("description", clean_text_udf(df_bio["description"]))
    df_bio = df_bio.withColumn("special_notes", clean_text_udf(df_bio["special_notes"]))
    df_results = df_results.withColumn("edition", clean_text_udf(df_results["edition"]))
    df_results = df_results.withColumn(
        "country_noc", clean_text_udf(df_results["country_noc"])
    )
    df_results = df_results.withColumn("sport", clean_text_udf(df_results["sport"]))
    df_results = df_results.withColumn("event", clean_text_udf(df_results["event"]))
    df_results = df_results.withColumn("athlete", clean_text_udf(df_results["athlete"]))
    df_results = df_results.withColumn("pos", clean_text_udf(df_results["pos"]))
    df_results = df_results.withColumn("medal", clean_text_udf(df_results["medal"]))

    # drop duplicate rows
    df_bio = df_bio.dropDuplicates()
    df_results = df_results.dropDuplicates()

    # display data
    df_bio.show(10)
    print("Numnber of rows in athlete_bio table: ", df_bio.count())
    print("------------------------------------------------------------------------")
    df_results.show(10)
    print("Numnber of rows in athlete_event_results table: ", df_results.count())

    # write datasets to parquet format
    df_bio.write.format("parquet").mode("overwrite").save("task_2/silver/athlete_bio")
    df_results.write.format("parquet").mode("overwrite").save(
        "task_2/silver/athlete_event_results"
    )

if __name__ == "__main__":
    main()