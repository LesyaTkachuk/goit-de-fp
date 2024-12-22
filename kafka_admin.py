from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
from conf.constants import GREEN, RED, LIGHT_RED, LIGHT_GREEN, END
import json

kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "jaas_config": "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';",
}

kafka_topic_config = {
    "user_name": "oleksandratk",
    "num_partions": 1,
    "replication_factor": 1,
}

# create Kafka client
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

def create_topic(topic_name):
    new_topic = NewTopic(
        name=topic_name,
        num_partitions=kafka_topic_config["num_partions"],
        replication_factor=kafka_topic_config["replication_factor"],
    )

    try:
        if topic_name in admin_client.list_topics():
            print(f"{GREEN}Topic {LIGHT_GREEN}{topic_name}{END} {GREEN}already exists.{END}")
            return
        admin_client.create_topics(
            new_topics=[
                new_topic
            ],
            validate_only=False,
        )
        print(
            f"{GREEN}Topics {LIGHT_GREEN}{new_topic}{END} {GREEN}created successfully.{END}"
        )
    except Exception as e:
        print(f"{LIGHT_RED}Error creating topic:{END} {RED}{e}{END} ")


# create Kafka Producer
def create_producer():

    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


# create Kafka Consumer
def create_consumer():

    return KafkaConsumer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # key_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",  # to start reading from the beginning
        enable_auto_commit=True,  # automatically commit message reading
    )


# "sensor_data_consumers_group"
kafka_topics = {
    "topic_athlete_event_results": f"{kafka_topic_config['user_name']}_athlete_event_results",
    "topic_athlete_enriched": f"{kafka_topic_config['user_name']}_athlete_enriched",
}
