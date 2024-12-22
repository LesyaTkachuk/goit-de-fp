from kafka_admin import create_consumer, kafka_topics
from conf.constants import GREEN, YELLOW, BLUE, RED, END

# create Kafka Consumer
alerts_consumer = create_consumer()

# get topic name for subscription
topic_athlete_enriched = kafka_topics["topic_athlete_enriched"]

# subscribe to topic_temperature_alerts and topic_humidity_alerts
alerts_consumer.subscribe([topic_athlete_enriched])
print(f"{GREEN}Subscribed to {topic_athlete_enriched}{END}")

try:
    for message in alerts_consumer:
        print(
            f"{BLUE}Received alert: {YELLOW}{message.value}, key: {YELLOW}{message.key}{END}"
        )

except KeyboardInterrupt:
    print(f"{RED}Keyboard interrupt received. Exiting...{END}")
except Exception as e:
    print(f"{RED}Error receiving alert: {e}{END}")
finally:
    alerts_consumer.close()
