# goit-de-fp

Data Engineering final project. Implemented MySql data processing flow, building end-to-end pipeline using Kafka, Spark and Airflow.

Part 1.

1. To read data from MySQL database and push them to Kafka topic run the next script:

```
python athlete_results_producer.py
```

2. To read data from both MySQL database and Kafka topic ru the next script. It causes the data join, batch processing in spark stream and enriched data microbatch pushing to both MySQL database and Kafka topic using FanOut pattern.

```
python streaming_pipeline.py
```

3. To read enriched data from Kafka topic run:

```
python kafka_consumer.py
```

The results of the enriched data pushing:

![kafka_consumer_data](screenshots/kafka_consumer.png)

![enriched_data_table](screenshots/enriched_data_table.png)
