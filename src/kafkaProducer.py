#Imports
from kafka import KafkaProducer
import json
import time
import requests
import helpers

# Docker Configuration
KAFKA_BROKER = 'kafka:29092'
TOPIC = 'satellite-positions'

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

producer = KafkaProducer(
    # Use of intern address 'kafka:29092'
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=1 # leader ensures data is received before going on
)

print(f"Producer démarré. Envoi vers {KAFKA_BROKER}...")

while True:
    try:
        # Get news data every 100s
        data = helpers.get_sat_data(SAT_ID, API_KEY, OB_LAT, OB_LON, OB_ALT, seconds=10)
        
        if data:
            producer.send(TOPIC, value=data)
            print("Sent:", data)
        else:
            print("Erreur : Données vides reçues de l'API.")
   
    except Exception as e:
        print(f"Erreur lors de l'envoi : {e}")

    time.sleep(100)  