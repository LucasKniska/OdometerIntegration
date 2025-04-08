# Run First
import json
import requests
import pandas as pd
import os
from geopy.distance import distance

# config
tenant = "torcroboticssb.us.accelix.com"
site = "def"

# For testing
# Cookie to the sandbox
sandbox_key = os.getenv("SANDBOX_KEY")
motive_key = os.getenv("MOTIVE_KEY")

print(sandbox_key)
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
            {"name": "id"},
            {"name": "geolocation"},
            {"name": "c_terminalzonedropdown"}
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

keysToCities = {
    'BCB': '351680e3-38fc-481a-b2e4-8a3834006c03', 
    'DFW': '9056cfa3-701d-46ed-8aec-37d2b642d2b4', 
    'UPG': '8eb92cad-6a68-4dc1-ae6e-58ced7eb34e0', 
    'Uvalde': 'ce169c81-ec66-4389-a3e2-b8382d993138', 
    'UPCG': 'ea0a16fd-36a5-4286-8103-1cc5871fd099', 
    'ABQ': '37589044-164f-4de2-b1aa-de4125247156', 
    '': 'f07a0860-8175-44e1-8364-3043762935dd', 
    'PDX': '83e7a2ec-da8d-4f9c-8a45-a3580bd1ac79'
}

def createTerminalZone(city):
    return {
        "entity": "TerminalZone",
        "id": keysToCities[city],
        "isDeleted": False,
        "number": 1,
        "title": city
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
        "c_terminalzonedropdown": createTerminalZone(truck['nearest_city'])
    }
    response = requests.put(url, headers=headers, data=json.dumps(data))
    
    if response.status_code != 200:
        print(f"Error updating {truck['id']}")
        return False
    
    return True


if __name__ == "__main__":
    print("Starting TerminalZoneUpdate.py")

    trucks = get_geolocations()

    # Apply the function to each row
    trucks['nearest_city'] = trucks['geolocation'].apply(get_nearest_city)

    print("\nNearest City - Truck ID")

    for i, truck in trucks.iterrows():

        if(truck['nearest_city'] is not None):
            post_nearest_city(truck)
            print(str(truck['nearest_city']) + " " + str(truck['id']))

    print("Done")