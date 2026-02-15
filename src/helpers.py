import time
import requests

def get_sat_data(sat_id, api_key, lat, lon, alt, seconds=2):
    """
    Fetch satellite data from the N2YO API.
    seconds parameter specifies how many seconds of data to retrieve (default is 2 to get near real time data).
    """
    url = f"https://api.n2yo.com/rest/v1/satellite/positions/{sat_id}/{lat}/{lon}/{alt}/{seconds}/&apiKey={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else: 
        print(f"Error fetching data: {response.status_code}")
        return None
    

def calculate_ground_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the ground distance between two points on Earth using the Haversine formula.
    """
    from math import radians, cos, sin, asin, sqrt

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371  # Radius of Earth in kilometers
    return c * r


    
