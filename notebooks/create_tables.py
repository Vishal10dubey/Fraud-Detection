from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://admin:admin@localhost:5433/fraud_detection"

engine = create_engine(DATABASE_URL)

create_table_query = """
CREATE TABLE IF NOT EXISTS fraud_transactions (

    id SERIAL PRIMARY KEY,

    transaction_id VARCHAR(255),
    user_id VARCHAR(255),

    amount FLOAT,
    merchant VARCHAR(255),
    location VARCHAR(255),

    device_id VARCHAR(255),

    transaction_time TIMESTAMP,

    is_fraud INTEGER,

    velocity INTEGER,
    location_change INTEGER,
    odd_hour INTEGER,
    high_amount INTEGER,

    fraud_score FLOAT,

    ml_prediction INTEGER,

    fraud_probability FLOAT,

    final_fraud BOOLEAN,

    fraud_label VARCHAR(50)
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table_query))
    conn.commit()

print("Table created successfully")