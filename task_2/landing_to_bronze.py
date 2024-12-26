import requests
from pyspark.sql import SparkSession
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))

# create spark session
spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()



def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + local_file_path + ".csv"
    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # check if the request was successful (status code 200)
    if response.status_code == 200:
        # open the local file in write-binary mode and write the content of the responce to it
        with open("task_2/bronze/" + local_file_path + ".csv", "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

def main():
    download_data("athlete_bio")
    download_data("athlete_event_results")

    print("Files downloaded successfully")
    # load dataset
    df_bio = spark.read.csv("task_2/bronze/athlete_bio.csv", header=True)
    df_results = spark.read.csv("task_2/bronze/athlete_event_results.csv", header=True)

    # display data
    df_bio.show(10)
    print("Numnber of rows in athlete_bio table: ", df_bio.count())
    print("------------------------------------------------------------------------")
    df_results.show(10)
    print("Numnber of rows in athlete_event_results table: ", df_results.count())

    # write datasets to parquet format
    df_bio.write.format("parquet").mode("overwrite").save("task_2/bronze/athlete_bio")
    df_results.write.format("parquet").mode("overwrite").save(
        "task_2/bronze/athlete_event_results"
    )

if __name__ == "__main__":
    main()