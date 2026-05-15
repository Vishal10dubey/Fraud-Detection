import pandas as pd
import random
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib

# Generate synthetic dataset
data = []

for _ in range(10000):

    amount = random.randint(100, 50000)
    velocity = random.randint(1, 10)
    location_change = random.randint(0, 1)
    odd_hour = random.randint(0, 1)

    fraud = 0

    # Fraud logic
    if amount > 30000:
        fraud = 1

    if velocity > 5:
        fraud = 1

    if location_change == 1 and amount > 10000:
        fraud = 1

    if odd_hour == 1 and amount > 15000:
        fraud = 1

    data.append([
        amount,
        velocity,
        location_change,
        odd_hour,
        fraud
    ])

df = pd.DataFrame(data, columns=[
    "amount",
    "velocity",
    "location_change",
    "odd_hour",
    "fraud"
])

X = df.drop("fraud", axis=1)
y = df["fraud"]

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42
)

model = RandomForestClassifier(
    n_estimators=100,
    random_state=42
)

model.fit(X_train, y_train)

predictions = model.predict(X_test)

print(classification_report(y_test, predictions))

# Save model
joblib.dump(model, "fraud_model.pkl")

print("Model saved successfully")