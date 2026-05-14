# from datetime import datetime

# # Rule 1 — Large transaction
# def large_transaction(txn):
#     return txn.get("amount") > 50000


# # Rule 2 — Foreign transaction
# def foreign_transaction(txn):
#     return txn.get("location") != txn.get("home_location")


# # Rule 3 — Night transaction
# def odd_hour_transaction(txn):

#     timestamp = txn.get("timestamp")

#     if not timestamp:
#         return False

#     hour = datetime.strptime(
#         timestamp,
#         "%Y-%m-%d %H:%M:%S"
#     ).hour

#     return 1 <= hour <= 5

# # Rule 4 — Rapid multiple transactions
# # Rule 4 — Rapid multiple transactions
# last_transaction_time = {}

# def rapid_transactions(txn):

#     user = txn.get("user_id")
#     timestamp = txn.get("timestamp")

#     if not user or not timestamp:
#         return False

#     current_time = datetime.strptime(
#         timestamp,
#         "%Y-%m-%d %H:%M:%S"
#     )

#     if user in last_transaction_time:

#         diff = (
#             current_time -
#             last_transaction_time[user]
#         ).seconds

#         last_transaction_time[user] = current_time

#         return diff < 10

#     last_transaction_time[user] = current_time

#     return False


#---------------------------------------Additional Rules---------------------------------------

from datetime import datetime, timedelta
from collections import defaultdict

# -----------------------------
# Rule 1 — Large transaction
# -----------------------------
def large_transaction(txn):

    amount = txn.get("amount")

    if amount is None:
        return False

    return amount > 50000


# -----------------------------
# Rule 2 — Foreign transaction
# -----------------------------
def foreign_transaction(txn):

    return (
        txn.get("location") !=
        txn.get("home_location")
    )


# -----------------------------
# Rule 3 — Odd hour transaction
# -----------------------------
def odd_hour_transaction(txn):

    timestamp = txn.get("timestamp")

    if not timestamp:
        return False

    hour = datetime.strptime(
        timestamp,
        "%Y-%m-%d %H:%M:%S"
    ).hour

    return 1 <= hour <= 5


# -----------------------------
# Rule 4 — Rapid transactions
# -----------------------------
last_transaction_time = {}

def rapid_transactions(txn):

    user = txn.get("user_id")
    timestamp = txn.get("timestamp")

    if not user or not timestamp:
        return False

    current_time = datetime.strptime(
        timestamp,
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


# -----------------------------
# Rule 5 — Velocity fraud
# -----------------------------
transaction_history = defaultdict(list)

def velocity_fraud(txn):

    user = txn.get("user_id")
    timestamp = txn.get("timestamp")

    if not user or not timestamp:
        return False

    current_time = datetime.strptime(
        timestamp,
        "%Y-%m-%d %H:%M:%S"
    )

    transaction_history[user].append(current_time)

    transaction_history[user] = [

        t for t in transaction_history[user]

        if current_time - t <= timedelta(minutes=1)
    ]

    return len(transaction_history[user]) > 5


# -----------------------------
# Rule 6 — Impossible travel
# -----------------------------
last_location = {}

def impossible_travel(txn):

    user = txn.get("user_id")
    location = txn.get("location")
    timestamp = txn.get("timestamp")

    if not user or not location or not timestamp:
        return False

    current_time = datetime.strptime(
        timestamp,
        "%Y-%m-%d %H:%M:%S"
    )

    if user in last_location:

        prev_location, prev_time = (
            last_location[user]
        )

        diff = (
            current_time - prev_time
        ).seconds / 60

        if prev_location != location and diff < 5:

            last_location[user] = (
                location,
                current_time
            )

            return True

    last_location[user] = (
        location,
        current_time
    )

    return False


# -----------------------------
# Rule 7 — High-risk merchant
# -----------------------------
HIGH_RISK_MERCHANTS = [
    "Crypto",
    "Betting",
    "DarkWeb",
    "Offshore"
]

def high_risk_merchant(txn):

    merchant = txn.get("merchant")

    return merchant in HIGH_RISK_MERCHANTS


# -----------------------------
# Rule 8 — Device change
# -----------------------------
user_devices = {}

def device_change(txn):

    user = txn.get("user_id")
    device = txn.get("device_id")

    if not user or not device:
        return False

    if user in user_devices:

        if user_devices[user] != device:

            user_devices[user] = device

            return True

    user_devices[user] = device

    return False


# -----------------------------
# Rule 9 — Card testing
# -----------------------------
small_txn_tracker = defaultdict(list)

def card_testing(txn):

    user = txn.get("user_id")
    amount = txn.get("amount")
    timestamp = txn.get("timestamp")

    if (
        not user or
        amount is None or
        not timestamp
    ):
        return False

    if amount > 100:
        return False

    current_time = datetime.strptime(
        timestamp,
        "%Y-%m-%d %H:%M:%S"
    )

    small_txn_tracker[user].append(
        current_time
    )

    small_txn_tracker[user] = [

        t for t in small_txn_tracker[user]

        if current_time - t <= timedelta(minutes=1)
    ]

    return len(small_txn_tracker[user]) > 3