import json
import random
from datetime import datetime
from collections import defaultdict

from kafka import KafkaConsumer, KafkaProducer
import joblib
import pandas as pd

from sqlalchemy import create_engine

DATABASE_URL = "postgresql://admin:admin@localhost:5433/fraud_detection"
engine = create_engine(DATABASE_URL)

def store_transaction(transaction):

    df = pd.DataFrame([transaction])

    df.to_sql(
        "fraud_transactions",
        engine,
        if_exists="append",
        index=False
    )


# Load trained ML model
model = joblib.load("spark_streaming/fraud_model.pkl")

# Kafka Consumer
consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Kafka Producer for fraud alerts
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

# Track user transaction count
user_transaction_count = defaultdict(int)


# def process_transaction(transaction):

#     user_id = transaction["user_id"]

#     # Update velocity count
#     user_transaction_count[user_id] += 1

#     velocity = user_transaction_count[user_id]

#     amount = transaction["amount"]

#     # Feature Engineering
#     location_change = 0
#     odd_hour = 0

#     current_hour = datetime.now().hour

#     if current_hour <= 5:
#         odd_hour = 1

#     # Simulate suspicious location change
#     if random.choice([True, False]):
#         location_change = 1

#     # Create ML features
#     features = pd.DataFrame([{
#         "amount": amount,
#         "velocity": velocity,
#         "location_change": location_change,
#         "odd_hour": odd_hour
#     }])

#     # ML Prediction
#     prediction = model.predict(features)[0]

#     probability = model.predict_proba(features)[0][1]

#     # Rule-Based Fraud Scoring
#     fraud_score = 0

#     if amount > 30000:
#         fraud_score += 40

#     if velocity > 5:
#         fraud_score += 30

#     if location_change:
#         fraud_score += 20

#     if odd_hour:
#         fraud_score += 10

#     # Final Fraud Decision
#     final_fraud = False

#     if fraud_score >= 70 or prediction == 1:
#         final_fraud = True

#     # Add results to transaction
#     transaction["velocity"] = velocity
#     transaction["location_change"] = location_change
#     transaction["odd_hour"] = odd_hour

#     transaction["fraud_score"] = fraud_score

#     transaction["ml_prediction"] = int(prediction)

#     transaction["fraud_probability"] = round(
#         probability * 100,
#         2
#     )

#     transaction["final_fraud"] = final_fraud

#     return transaction

def process_transaction(transaction):

    user_id = transaction["user_id"]

    # Track transaction velocity
    user_transaction_count[user_id] += 1

    velocity = user_transaction_count[user_id]

    amount = transaction["amount"]

    # -----------------------------
    # Feature Engineering
    # -----------------------------

    # Feature 1 — Odd Hour
    odd_hour = 0

    current_hour = datetime.now().hour

    if current_hour <= 5:
        odd_hour = 1

    # Feature 2 — Location Change
    location_change = 0

    # Simulated anomaly
    if random.choice([True, False]):
        location_change = 1

    # Feature 3 — High Amount
    high_amount = 0

    if amount > 25000:
        high_amount = 1

    # -----------------------------
    # Create ML Feature DataFrame
    # -----------------------------

    features = pd.DataFrame([{
        "amount": amount,
        "velocity": velocity,
        "location_change": location_change,
        "odd_hour": odd_hour
    }])

    # -----------------------------
    # ML Prediction
    # -----------------------------

    prediction = model.predict(features)[0]

    probability = model.predict_proba(features)[0][1]

    # -----------------------------
    # Rule-Based Fraud Scoring
    # -----------------------------

    fraud_score = 0

    if amount > 30000:
        fraud_score += 40

    if velocity > 5:
        fraud_score += 30

    if location_change:
        fraud_score += 20

    if odd_hour:
        fraud_score += 10

    if high_amount:
        fraud_score += 10

    # -----------------------------
    # Final Fraud Decision
    # -----------------------------

    # final_fraud = False

    # if fraud_score >= 70 or prediction == 1:
    #     final_fraud = True


# -----------------------------
# Combine Rules + ML
# -----------------------------

    final_fraud = False

# High confidence fraud
    if fraud_score >= 70:
        final_fraud = True

# ML predicts fraud strongly
    elif prediction == 1 and probability >= 0.80:
        final_fraud = True

# Medium-risk hybrid case
    elif fraud_score >= 40 and probability >= 0.60:
        final_fraud = True

    

    # -----------------------------
    # Add Results To Transaction
    # -----------------------------

    transaction["velocity"] = velocity

    transaction["location_change"] = location_change

    transaction["odd_hour"] = odd_hour

    transaction["high_amount"] = high_amount

    transaction["fraud_score"] = fraud_score

    transaction["ml_prediction"] = int(prediction)

    transaction["fraud_probability"] = round(
        probability * 100,
        2
    )

    

    # Store final fraud result
    transaction["final_fraud"] = final_fraud

# Fraud label
    if final_fraud:
        transaction["fraud_label"] = "FRAUD"
    else:
        transaction["fraud_label"] = "GENUINE"

    return transaction

print("Listening for transactions...")


# Consume Kafka messages
for message in consumer:

    transaction = message.value

    processed_transaction = process_transaction(
        transaction
    )

    print(processed_transaction)

    store_transaction(processed_transaction)

    # Send fraud alerts
    if processed_transaction["final_fraud"]:

        producer.send(
            "fraud_alerts",
            value=processed_transaction
        )

        print(
            f"FRAUD ALERT: {processed_transaction}"
        )