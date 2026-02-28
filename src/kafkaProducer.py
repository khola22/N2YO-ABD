from kafka import KafkaProducer
import json
import time
import requests

import helpers

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'satellite-positions'

# # Example data (replace with API call)
# data = {
#     'info': {'satname': 'SPACE STATION', 'satid': 25544, 'transactionscount': 0},
#     'positions': [
#         {'satlatitude': 51.75853274, 'satlongitude': 178.21328368, 'sataltitude': 419.54, 'timestamp': 1771186481}
#     ]
# }

# API call 
# API configuration
API_KEY = "9MGGM6-VSQ68S-U4M6E8-5NMO"
SAT_ID = "25544"  # ISS (International Space Station)
OB_LAT, OB_LON, OB_ALT = 40.7128, -74.0060, 0  # Observer's location (New York City)

data = helpers.get_sat_data(SAT_ID, API_KEY, OB_LAT, OB_LON, OB_ALT)

# print all the data
print(data)

while True:
    producer.send(topic, value=data)
    print("Sent:", data)
    time.sleep(500)  # Send every 300 seconds