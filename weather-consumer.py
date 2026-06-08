import json
import time
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Connect to Kafka
while True:
    try:
        consumer = KafkaConsumer(
            'weather',
            bootstrap_servers=['kafka:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='weather-group'
        )
        print("Connected to Kafka!")
        break
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 5 seconds...")
        time.sleep(5)

# Connect to PostgreSQL
while True:
    try:
        conn = psycopg2.connect(
            dbname='weatherdb',
            user='user',
            password='password',
            host='postgres',
            port='5432'
        )
        cur = conn.cursor()
        print("Connected to PostgreSQL!")
        break
    except Exception as e:
        print("Postgres not ready, retrying in 5 seconds...")
        print(e)
        time.sleep(5)

# Create table
cur.execute("""
CREATE TABLE IF NOT EXISTS weather_data(
    id SERIAL PRIMARY KEY,
    city TEXT,
    temperature FLOAT,
    condition TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()

# Consume messages
for message in consumer:
    weather = message.value
    try:
        city = weather['location']['name']
        temp = weather['current']['temp_c']
        condition = weather['current']['condition']['text']

        cur.execute(
            "INSERT INTO weather_data(city, temperature, condition) VALUES (%s, %s, %s)",
            (city, temp, condition)
        )

        conn.commit()
        print(f"Inserted: {city}, {temp}, {condition}")

    except Exception as e:
        print("Insert error:", e)