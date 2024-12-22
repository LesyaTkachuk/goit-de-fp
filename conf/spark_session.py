from pyspark.sql import SparkSession

# create spark session
spark = (
    SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4")
    .appName("JDBCToKafkaPipline")
    .getOrCreate()
)
