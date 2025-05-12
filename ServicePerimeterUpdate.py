# Run First
import json
import requests
import pandas as pd
import os
from geopy.distance import distance

production = True

# config
tenant = "torcrobotics.us.accelix.com" if production else "torcroboticssb.us.accelix.com"
site = "def"

# Cookie to the sandbox
sandbox_key = "JWT-Bearer=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3NTQiIsImV4cCI6NDEwMjQ0NDgwMCwic2lkIjpudWxsLCJpaWQiOm51bGx9.94frut80sKx43Cm4YKfVbel8upAQ8glWdfYIN3tMF7A"
production_key = "JWT-Bearer=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3MiLCJleHAiOjQxMDI0NDQ4MDAsInNpZCI6bnVsbCwiaWlkIjpudWxsfQ.Gh3b3ibvSeYy7YpqDUI9daup86dYjsM_lisS-8ESWDs"
motive_key = "9e90504a-82f0-4ed4-b54c-ce37f388f211"

headers = {'Content-Type': 'application/json', 'Cookie': production_key if production else sandbox_key}


def get_geolocations():
    """
    Gets all of the freightliners and trailer assets from fluke.

    Returns:
        pandas.DataFrame: A DataFrame containing the following columns for each asset:
            - 'c_description': Number of the truck (ex: C19 - Mill Mountain).
            - 'c_assettype': The type of the asset (either 'Freightliner' or 'Trailer').
            - 'id': The unique identifier of the asset.

    """

    # Get the freightliner assets
    url = f'https://{tenant}/api/entities/{site}/Assets/search-paged'

    data = {
        "select": [
            {"name": "c_description"},
            {"name": "id"},
            {"name": "geolocation"},
            {"name": "c_serviceperimeter"}
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
        print("Error: getting Freightliners", flush=True)
        return False
    
    response = response.json()
    dx = response['data']
    pages = response['totalPages']
    for page in range(1, pages):
        data['page'] = page
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            print("Error: getting Freightliners", flush=True)
            return False
        dx.extend(response.json()['data'])

        # dataframe
    df = pd.DataFrame(data={cx: [x[cx] for x in dx] for cx in sorted(dx[0].keys())})

    return df

# Define cities and their coordinates
CITIES = {
    'PDX (Portland, OR)': (45.5152, -122.6784), 
    'MTL (Montreal, CN)': (45.4636, -73.6177), 
    'MPG (Michellin Proving Grounds, SC)': (34.3925, -82.0291),
    'ACM (American Center for Mobilities, MI)': (42.2381, -83.5537),
    'UPG (Uvalde Proving Grounds, TX)': (29.1125, -99.7521),
    'BCB (Blacksburg, VA)': (37.1897, -80.3921),
    'DFW (Dallas, TX)': (32.9669, -97.2983),
    'AUS (Austin, TX)': (30.0667, -97.8357),
    'ARB (Ann Arbor, MI)': (42.3057, -83.6842) # Need full name of this one   
}

KEYS_TO_CITIES = {
    'PDX (Portland, OR)': '90d55c73-1cfb-48a2-942f-d49edab539bd',
    'MTL (Montreal, CN)': 'c3d8de75-d3ee-46b1-b5ba-06f9de164a4e',
    'MPG (Michellin Proving Grounds, SC)': 'c71c2cc2-201c-4ab5-ba47-1863f4b288fa',
    'ACM (American Center for Mobilities, MI)': 'cc385a67-48bc-4cda-aeb8-9a16c7b0f623',
    'UPG (Uvalde Proving Grounds, TX)': '1bd9fded-1eea-4712-92a8-b9b80a6a4aa0',
    'BCB (Blacksburg, VA)': '8bf9e772-1235-4146-8dde-cc4efd0aff5d',
    'DFW (Dallas, TX)': '9bc84368-8830-48b9-a1c6-214823c30308',
    'AUS (Austin, TX)': '7fdb0a74-124f-43e1-9605-d764cb774c61',
    'ARB (Ann Arbor, MI)': '0a9c6d50-88d9-41a1-9787-5d6df3db0519',
}

def createServicePerimeter(city):
    return {
        "entity": "ServicePerimeter",
        "id": KEYS_TO_CITIES[city], # Set this to a constnat id to see if it works for a specific truck
        "isDeleted": False,
        "number": 1,
        "title": city
    }

# Function to get nearest city using geopy
def getNearestCity(location):
    if location is None:
        return None
    try:
        loc = (location['lat'], location['long'])
    except:
        print("Error: Could not process: ", location, flush=True)
        return None
    
    nearest = min(CITIES.items(), key=lambda city: distance(loc, city[1]).km)
    return nearest[0]

# Post the 'nearest_city' field to the associated 'id' for each truck
def postNearestCity(truck):
    url = f'https://{tenant}/api/entities/{site}/Assets/{truck["id"]}'
    
    data = {
        "properties": {
            "c_serviceperimeter": createServicePerimeter(truck['nearest_city'])
        },
    }
    response = requests.put(url, headers=headers, data=json.dumps(data))
    
    if response.status_code != 200:
        print(f"Error updating {truck['id']}", flush=True)
        print(response.json())
        return False
    
    return True


if __name__ == "__main__":
    trucks = get_geolocations()

    # Apply the function to each row
    trucks['nearest_city'] = trucks['geolocation'].apply(getNearestCity)

    for i, truck in trucks.iterrows():

        if(truck['nearest_city'] is not None):
            print("Truck: ", truck['c_description'], "Nearest City: ", truck['nearest_city'], flush=True)
            postNearestCity(truck)

    print("Run Complete", flush=True)