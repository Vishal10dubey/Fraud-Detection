import json

from kafka import KafkaConsumer, KafkaProducer

from config.kafka_config import (
    KAFKA_BROKER,
    TRANSACTION_TOPIC,
    FRAUD_TOPIC
)

from detector.rules import (
    large_transaction,
    foreign_transaction,
    odd_hour_transaction,
    rapid_transactions
)

consumer = KafkaConsumer(
    TRANSACTION_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="fraud-detection-group"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

print("Fraud detector running...")

for message in consumer:

    txn = message.value

    fraud_reasons = []

    # Apply rules
    if large_transaction(txn):
        fraud_reasons.append("Large Transaction")

    if foreign_transaction(txn):
        fraud_reasons.append("Foreign Transaction")

    if odd_hour_transaction(txn):
        fraud_reasons.append("Odd Hour Transaction")

    if rapid_transactions(txn):
        fraud_reasons.append("Rapid Transactions")

    txn["fraud"] = len(fraud_reasons) > 0
    txn["fraud_reasons"] = fraud_reasons

    print(txn)

    producer.send(FRAUD_TOPIC, txn)