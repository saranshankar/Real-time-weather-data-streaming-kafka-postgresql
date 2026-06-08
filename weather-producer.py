import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

API_KEY = "522e2854069649fd9f883402262905"
CITY = "Dharmapuri"

URL = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}&aqi=no"

# Connect to Kafka
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Producer connected to Kafka!")
        break

    except NoBrokersAvailable:
        print("Kafka not ready for producer, retrying in 5 seconds...")
        time.sleep(5)

# Fetch weather data and send to Kafka
while True:
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()

        data = response.json()

        producer.send("weather", value=data)
        producer.flush()

        print(
            f"Sent weather data: "
            f"{data['location']['name']} | "
            f"{data['current']['temp_c']}°C | "
            f"{data['current']['condition']['text']}"
        )

        time.sleep(10)

    except requests.exceptions.RequestException as e:
        print("Weather API error:", e)
        time.sleep(10)

    except Exception as e:
        print("Producer error:", e)
        time.sleep(10)