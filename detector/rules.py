from datetime import datetime

# Rule 1 — Large transaction
def large_transaction(txn):
    return txn["amount"] > 50000


# Rule 2 — Foreign transaction
def foreign_transaction(txn):
    return txn["location"] != txn["home_location"]


# Rule 3 — Night transaction
def odd_hour_transaction(txn):
    hour = datetime.strptime(
        txn["timestamp"],
        "%Y-%m-%d %H:%M:%S"
    ).hour

    return hour >= 1 and hour <= 5


# Rule 4 — Rapid multiple transactions
last_transaction_time = {}

def rapid_transactions(txn):
    user = txn["user_id"]

    current_time = datetime.strptime(
        txn["timestamp"],
        "%Y-%m-%d %H:%M:%S"
    )

    if user in last_transaction_time:
        diff = (
            current_time -
            last_transaction_time[user]
        ).seconds

        last_transaction_time[user] = current_time

        return diff < 10

    last_transaction_time[user] = current_time
    return False