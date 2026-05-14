# import json

# from kafka import KafkaConsumer, KafkaProducer

# from configs.kafka_config import (
#     KAFKA_BROKER,
#     TRANSACTION_TOPIC,
#     FRAUD_TOPIC
# )

# from configs.rules_config import (
#     large_transaction,
#     foreign_transaction,
#     odd_hour_transaction,
#     rapid_transactions
# )

# consumer = KafkaConsumer(
#     TRANSACTION_TOPIC,
#     bootstrap_servers=KAFKA_BROKER,
#     value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#     auto_offset_reset="latest",
#     group_id="fraud-detection-group"
# )

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda x: json.dumps(x).encode("utf-8")
# )

# print("Fraud detector running...")

# for message in consumer:

#     txn = message.value

#     fraud_reasons = []

#     # Apply rules
#     if large_transaction(txn):
#         fraud_reasons.append("Large Transaction")

#     if foreign_transaction(txn):
#         fraud_reasons.append("Foreign Transaction")

#     if odd_hour_transaction(txn):
#         fraud_reasons.append("Odd Hour Transaction")

#     if rapid_transactions(txn):
#         fraud_reasons.append("Rapid Transactions")

#     txn["fraud"] = len(fraud_reasons) > 0
#     txn["fraud_reasons"] = fraud_reasons

#     print(txn)

#     producer.send(FRAUD_TOPIC, txn)

#------------------------------------------------------------------Additional ----------------------------------------

 #import json

# from kafka import KafkaConsumer, KafkaProducer

# from configs.kafka_config import (
#     KAFKA_BROKER,
#     TRANSACTION_TOPIC,
#     FRAUD_TOPIC
# )

# from detector.rules import (
#     large_transaction,
#     foreign_transaction,
#     odd_hour_transaction,
#     rapid_transactions
# )

# consumer = KafkaConsumer(
#     TRANSACTION_TOPIC,
#     bootstrap_servers=KAFKA_BROKER,
#     value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#     auto_offset_reset="latest",
#     group_id="fraud-detection-group"
# )

# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda x: json.dumps(x).encode("utf-8")
# )

# print("Fraud detector running...")

# for message in consumer:

#     txn = message.value

#     fraud_reasons = []

#     # Apply rules
#     if large_transaction(txn):
#         fraud_reasons.append("Large Transaction")

#     if foreign_transaction(txn):
#         fraud_reasons.append("Foreign Transaction")

#     if odd_hour_transaction(txn):
#         fraud_reasons.append("Odd Hour Transaction")

#     if rapid_transactions(txn):
#         fraud_reasons.append("Rapid Transactions")

#     txn["fraud"] = len(fraud_reasons) > 0
#     txn["fraud_reasons"] = fraud_reasons

#     print(txn)

#     producer.send(FRAUD_TOPIC, txn)

#-------------------------------------------------------Fraud detector with Kafka setup---------------------------------------
# import json

# from kafka import (
#     KafkaConsumer,
#     KafkaProducer
# )

# from loguru import logger

# from configs.kafka_config import (
#     KAFKA_BROKER,
#     TRANSACTION_TOPIC,
#     FRAUD_TOPIC
# )

# from detector.rules import (

#     large_transaction,
#     foreign_transaction,
#     odd_hour_transaction,
#     rapid_transactions,
#     velocity_fraud,
#     impossible_travel,
#     high_risk_merchant,
#     device_change,
#     card_testing
# )

# # -----------------------------------
# # Kafka Consumer
# # -----------------------------------
# consumer = KafkaConsumer(

#     TRANSACTION_TOPIC,

#     bootstrap_servers=KAFKA_BROKER,

#     value_deserializer=lambda x:
#     json.loads(x.decode("utf-8")),

#     auto_offset_reset="latest",

#     group_id="fraud-detection-group"
# )

# # -----------------------------------
# # Kafka Producer
# # -----------------------------------
# producer = KafkaProducer(

#     bootstrap_servers=KAFKA_BROKER,

#     value_serializer=lambda x:
#     json.dumps(x).encode("utf-8")
# )

# logger.info(
#     "Fraud detector running..."
# )

# # -----------------------------------
# # Stream Processing
# # -----------------------------------
# for message in consumer:

#     try:

#         txn = message.value

#         risk_score = 0

#         fraud_reasons = []

#         # -----------------------------
#         # Basic Rules
#         # -----------------------------
#         if large_transaction(txn):

#             risk_score += 40

#             fraud_reasons.append(
#                 "Large Transaction"
#             )

#         if foreign_transaction(txn):

#             risk_score += 30

#             fraud_reasons.append(
#                 "Foreign Transaction"
#             )

#         if odd_hour_transaction(txn):

#             risk_score += 10

#             fraud_reasons.append(
#                 "Odd Hour Transaction"
#             )

#         if rapid_transactions(txn):

#             risk_score += 20

#             fraud_reasons.append(
#                 "Rapid Transactions"
#             )

#         # -----------------------------
#         # Advanced Rules
#         # -----------------------------
#         if velocity_fraud(txn):

#             risk_score += 25

#             fraud_reasons.append(
#                 "Velocity Fraud"
#             )

#         if impossible_travel(txn):

#             risk_score += 40

#             fraud_reasons.append(
#                 "Impossible Travel"
#             )

#         if high_risk_merchant(txn):

#             risk_score += 30

#             fraud_reasons.append(
#                 "High Risk Merchant"
#             )

#         if device_change(txn):

#             risk_score += 20

#             fraud_reasons.append(
#                 "Device Change"
#             )

#         if card_testing(txn):

#             risk_score += 35

#             fraud_reasons.append(
#                 "Card Testing"
#             )

#         # -----------------------------
#         # Final Fraud Decision
#         # -----------------------------
#         txn["risk_score"] = risk_score

#         txn["fraud"] = (
#             risk_score >= 50
#         )

#         txn["fraud_reasons"] = (
#             fraud_reasons
#         )

#         # --------------------------------
#         # Send to fraud topic
#         # --------------------------------
#         producer.send(
#             FRAUD_TOPIC,
#             txn
#         )

#         producer.flush()

#         logger.info(txn)

#         if txn["fraud"]:

#             logger.warning(
#                 f"Fraud detected for "
#                 f"{txn['user_id']}"
#             )

#     except Exception as e:

#         logger.error(
#             f"Error processing "
#             f"transaction: {e}"
#         )

#-----------------------------------------Adding logging and advanced rules-----------------------------------------

import json

from kafka import (
    KafkaConsumer,
    KafkaProducer
)

from loguru import logger

from configs.kafka_config import (
    KAFKA_BROKER,
    TRANSACTION_TOPIC,
    FRAUD_TOPIC
)

from detector.rules import (

    large_transaction,
    foreign_transaction,
    odd_hour_transaction,
    rapid_transactions,
    velocity_fraud,
    impossible_travel,
    high_risk_merchant,
    device_change,
    card_testing
)

# -----------------------------------
# Kafka Consumer
# -----------------------------------
consumer = KafkaConsumer(

    TRANSACTION_TOPIC,

    bootstrap_servers=KAFKA_BROKER,

    value_deserializer=lambda x:
    json.loads(x.decode("utf-8")),

    auto_offset_reset="latest",

    group_id="fraud-detection-group"
)

# -----------------------------------
# Kafka Producer
# -----------------------------------
producer = KafkaProducer(

    bootstrap_servers=KAFKA_BROKER,

    value_serializer=lambda x:
    json.dumps(x).encode("utf-8")
)

# -----------------------------------
# Logging Configuration
# -----------------------------------
import os

os.makedirs("logs", exist_ok=True)

logger.add(
    r"E:\Projects\Fraud detection\Fraud detection\logs\fraud_detector.log",
    rotation="10 MB",
    level="INFO"
)

logger.info(
    "Fraud detector started..."
)

# -----------------------------------
# Stream Processing
# -----------------------------------
for message in consumer:

    try:

        txn = message.value

        logger.info(
            f"Received transaction: {txn}"
        )

        risk_score = 0

        fraud_reasons = []

        # --------------------------------
        # Basic Rules
        # --------------------------------
        if large_transaction(txn):

            risk_score += 40

            fraud_reasons.append(
                "Large Transaction"
            )

        if foreign_transaction(txn):

            risk_score += 30

            fraud_reasons.append(
                "Foreign Transaction"
            )

        if odd_hour_transaction(txn):

            risk_score += 10

            fraud_reasons.append(
                "Odd Hour Transaction"
            )

        if rapid_transactions(txn):

            risk_score += 20

            fraud_reasons.append(
                "Rapid Transactions"
            )

        # --------------------------------
        # Advanced Rules
        # --------------------------------
        if velocity_fraud(txn):

            risk_score += 25

            fraud_reasons.append(
                "Velocity Fraud"
            )

        if impossible_travel(txn):

            risk_score += 40

            fraud_reasons.append(
                "Impossible Travel"
            )

        if high_risk_merchant(txn):

            risk_score += 30

            fraud_reasons.append(
                "High Risk Merchant"
            )

        if device_change(txn):

            risk_score += 20

            fraud_reasons.append(
                "Device Change"
            )

        if card_testing(txn):

            risk_score += 35

            fraud_reasons.append(
                "Card Testing"
            )

        # --------------------------------
        # Final Fraud Decision
        # --------------------------------
        txn["risk_score"] = risk_score

        txn["fraud"] = (
            risk_score >= 50
        )

        txn["fraud_reasons"] = (
            fraud_reasons
        )

        # --------------------------------
        # Send to Kafka
        # --------------------------------
        producer.send(
            FRAUD_TOPIC,
            txn
        )

        producer.flush()

        logger.info(
            f"Processed transaction: {txn}"
        )

        # --------------------------------
        # Fraud Alert Logging
        # --------------------------------
        if txn["fraud"]:

            logger.warning(

                f"FRAUD DETECTED | "

                f"user={txn.get('user_id')} | "

                f"score={risk_score} | "

                f"reasons={fraud_reasons}"
            )

    # --------------------------------
    # Error Handling
    # --------------------------------
    except Exception as e:

        logger.error(

            f"Error processing transaction: {e}"
        )