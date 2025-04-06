# Run First
import json
import requests
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime
import math
from zoneinfo import ZoneInfo

from geopy.distance import distance

# config
tenant = "torcroboticssb.us.accelix.com"
site = "def"

# For testing
sandbox_key = "JWT-Bearer=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3NTQiIsImV4cCI6NDEwMjQ0NDgwMCwic2lkIjpudWxsLCJpaWQiOm51bGx9.94frut80sKx43Cm4YKfVbel8upAQ8glWdfYIN3tMF7A"
motive_key = "9e90504a-82f0-4ed4-b54c-ce37f388f211"

headers = {'Content-Type': 'application/json', 'Cookie': sandbox_key}


def get_geolocations():
    """
    Gets all of the freightliners and trailer assets from fluke.

    Returns:
        pandas.DataFrame: A DataFrame containing the following columns for each asset:
            - 'c_description': Number of the truck (ex: C19 - Mill Mountain).
            - 'c_assettype': The type of the asset (either 'Freightliner' or 'Trailer').
            - 'id': The unique identifier of the asset.

    """

    print("Getting Asset Information")
    # Get the freightliner assets
    url = f'https://{tenant}/api/entities/{site}/Assets/search-paged'

    data = {
        "select": [
            {"name": "c_description"},
            {"name": "c_assettype"},
            {"name": "id"},
            {"name": "geolocation"},
            {"name": "c_terminalZone"}
        ],
        "filter": {
            "and": [
                {"name": "isDeleted", "op": "isfalse"},
                {"name": "c_assettype", "op": "eq", "value": "Freightliner"},
            ],
        },
        "order": [],
        "pageSize": 50,
        "page": 0,
        "fkExpansion": True
    }

    # API
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.status_code != 200:
        print("Error getting Freightliners")
        return False
    
    response = response.json()
    dx = response['data']
    pages = response['totalPages']
    for page in range(1, pages):
        data['page'] = page
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            print("Error getting Freightliners")
            return False
        dx.extend(response.json()['data'])

        # dataframe
    df = pd.DataFrame(data={cx: [x[cx] for x in dx] for cx in sorted(dx[0].keys())})

    return df

# Define cities and their coordinates
cities = {
    'ABQ': (35.0844, -106.6504),
    'BCB': (37.2296, -80.4139),
    'DFW': (32.8998, -97.0403),
    'UPG': (29.2097, -99.7862),
    'PDX': (45.5152, -122.6784)
}

# Function to get nearest city using geopy
def get_nearest_city(location):
    if location is None:
        return None
    try:
        loc = (location['lat'], location['long'])
    except:
        print("Could not process: ", location)
        return None
    
    nearest = min(cities.items(), key=lambda city: distance(loc, city[1]).km)
    return nearest[0]

# Post the 'nearest_city' field to the associated 'id' for each truck
def post_nearest_city(truck):
    url = f'https://{tenant}/api/entities/{site}/Assets/{truck["id"]}'
    data = {
        "c_terminalZone": truck['nearest_city']
    }
    response = requests.put(url, headers=headers, data=json.dumps(data))
    
    if response.status_code != 200:
        print(f"Error updating {truck['id']}")
        return False
    
    return True


if __name__ == "main":

    trucks = get_geolocations()

    # Apply the function to each row
    trucks['nearest_city'] = trucks['geolocation'].apply(get_nearest_city)

    for i, truck in trucks.iterrows():

        if(truck['nearest_city'] is not None):
            post_nearest_city(truck)
            print(str(truck['nearest_city']) + " " + str(truck['id']))
