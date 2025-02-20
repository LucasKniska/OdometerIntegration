# TODO - Documentation, Get rid of tqdm

# Get all the vehicle location information
import json
import requests
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime
import math

# config
tenant = "torcroboticssb.us.accelix.com"
site = "def"

sandbox_key = os.getenv("SANDBOX_KEY")
# production_key = os.getenv("PRODUCTION_KEY")
motive_key = os.getenv("MOTIVE_KEY")

# For testing
sandbox_key = "JWT-Bearer=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiI5NWZkYzZhYS0wOWNiLTQ0NzMtYTIxZC1kNzBiZTE2NWExODMiLCJ0aWQiOiJUb3JjUm9ib3RpY3NTQiIsImV4cCI6NDEwMjQ0NDgwMCwic2lkIjpudWxsLCJpaWQiOm51bGx9.94frut80sKx43Cm4YKfVbel8upAQ8glWdfYIN3tMF7A"
motive_key = "9e90504a-82f0-4ed4-b54c-ce37f388f211"


headers = {'Content-Type': 'application/json', 'Cookie': sandbox_key}

def getMotiveOdometerValues():
    odometer_endpoint = f"https://api.gomotive.com/v2/vehicle_locations?page_no=1"

    motive_headers = {
        "accept": "application/json", 
        "X-Api-Key": motive_key
    }

    response = requests.get(odometer_endpoint, headers=motive_headers)

    pagination = response.json()['pagination']
    pages = math.ceil(pagination['total']/pagination['per_page'])

    def get_odometers(page_no):
        odometer_endpoint = f"https://api.gomotive.com/v2/vehicle_locations?page_no={page_no}"

        response = requests.get(odometer_endpoint, headers=motive_headers)

        return response.json()

    odometer_vals = get_odometers(1)

    # Starts on one, goes until the last page
    for i in range(2, pages+1):
        response2 = get_odometers(i)
        for vehicle in response2['vehicles']:
            
            odometer_vals['vehicles'].append(vehicle)

    # turns data into something digestible
    odometer_readings = []
    for vehicle in odometer_vals['vehicles']:
        cursor = vehicle['vehicle']

        if(cursor['current_location'] is not None):
            odometer_readings.append([cursor['number'], cursor['current_location']['odometer']])
        else:
            print(f"Vehicle {cursor['number']} has no odometer reading")

    return odometer_readings

# Gets all of the truck ids from fluke
def getAllTruckAssets():
    assets_url = 'https://torcroboticssb.us.accelix.com/api/entities/def/Assets/search-paged'

    data = {
        "select": [
            {"name": "c_serialnumber"},
            {"name": "number"},
            {"name": "c_description"},
            {"name": "c_assettype"}
        ],
        "filter": {
            "and": [
                {"name": "isDeleted", "op": "isfalse"}
            ]
        },
        "order": [
            {"name": "c_serialnumber", "desc": True}
        ],
        "pageSize": 20,
        "page": 0,
        "fkExpansion": True
    }

    # API
    response = requests.post(assets_url, headers=headers, data=json.dumps(data))
    assert response.status_code == 200
    response = response.json()
    dx = response['data']
    pages = response['totalPages']
    for page in tqdm(range(1, pages), desc='Assets'):
        data['page'] = page
        response = requests.post(assets_url, headers=headers, data=json.dumps(data))
        assert response.status_code == 200
        dx.extend(response.json()['data'])

    # dataframe
    df = pd.DataFrame(data={cx: [x[cx] for x in dx] for cx in sorted(dx[0].keys())})

    # Gets all of the truck id numbers
    def getAllTruckIds(): 
        freightlinerId = 'b6d90bff-dd0e-46f9-969b-d089f5983957' # In c_assettype > id

        freightliner_rows = []

        # Iterate through rows
        for _, row in df.iterrows():
            try:
                if row["c_assettype"].get("id") == freightlinerId:
                    freightliner_rows.append(row.to_dict())  # Save row as dictionary
            except json.JSONDecodeError:
                continue  # Skip if parsing fails

        return freightliner_rows

    return getAllTruckIds()

# Inserts odometer values into fluke data
def insertOdometerValues(odometer_data, asset_data):

    # Match motive truck/value with correct fluke asset
    def km_to_mile(km):
        return round(km * 0.621371, 2)

    
    # Iterate through the odometer data and match with asset dictionary
    for row in odometer_data:

        key = row[0] # The Truck Number
        value = row[1] # The row 
        
        assetKey = key.split(' ')[0]  # Extract the first part of the key
        
        for asset in asset_data:
            truckName = asset['c_description']

            if assetKey in truckName:
                if value != "N/A" and value != None:
                    asset['odometer_value'] = km_to_mile(value)  # Add odometer value
                    asset['truck_from_motive'] = assetKey
    
    return list(asset_data)  # Convert back to a list

# Get all related fields from trucks
def getAllMeterInfos(assets: list) -> list[list, list, list]:

    freightliner_relatedInfo = []
    freightliners_withoutmeter = []
    freightliners_withoutOdometer = []

    def getRelatedInfo(asset: dict):
        assetId = asset['id']

        fluke = f"https://{tenant}/api/entities/{site}/Assets/{assetId}?includeRelated=true" # Include related to get odometer field information

        asset_response = requests.get(fluke, headers=headers)

        r = json.loads(asset_response.text)

        try: 
            necessary_info = {
                "id": r['properties']['id'],
                "description": asset['truck_from_motive'],
                'meterId': r['related']['AssetMeters'][0]['properties']['id'],
                'odometerValue': asset['odometer_value']
                
            }
            freightliner_relatedInfo.append(necessary_info)

            print(r['properties']['c_description'] + ": Ready to go")
        except Exception as e:
            try:

                necessary_info = {
                    "id": r['properties']['id'],
                    "description": asset['truck_from_motive'],
                    'odometerValue': asset['odometer_value']
                }

                freightliners_withoutmeter.append(necessary_info)

                print(r['properties']['c_description'] + ": No meter")

            except Exception as err:

                necessary_info = {
                    "id": r['properties']['id'],
                    "description": r['properties']['c_description'],
                }
                
                freightliners_withoutOdometer.append(necessary_info)

                print(r['properties']['c_description'] + ": No odometer")

    for asset in assets:
        getRelatedInfo(asset)

    return [freightliner_relatedInfo, freightliners_withoutmeter, freightliners_withoutOdometer]

# Updates odometer readings througha asset meter readings
def UpdateOdometerReadings(trucks: list): # Only data from freightliner_info["relatedInfo"] should be passed here
    # EDIT ASSET METER - With Truck ID and Meter ID
    for truck in trucks:
        # ADD ASSET METER READING
        assetMeterReadings = f'https://{tenant}/api/entities/{site}/AssetMeterReadings'

        payload = {
            "properties": {
                "date": str(datetime.now()),
                "assetMeterId": truck['meterId'],
                "value": truck['odometerValue']
            },
            "related": {},
            "deleted": False
        }

        response = requests.post(url=assetMeterReadings, headers=headers, data=json.dumps(payload))

        try: 
            # Check response
            if response.status_code == 200 or response.status_code == 201:
                print(truck['description'] + ": Asset Meter updated successfully!")
                print(response.json())
            else:
                print(f"Failed. Status code: {response.status_code}")
                print("Error:", response.text)
                print(response)
        except Exception as e:
            pass

# makes a new odometer meter and updates it with new value
def MakeOdometerMeter(trucks: list): # Only data from freightliner_info["withoutMeterId"] should be passed here

    # The type of meter being created
    meter_type_id = "6330cf04-5555-44b7-aad8-a843d9e438d1"

    def UploadingOdometerMeter(truck):
        fluke = f"https://{tenant}/api/entities/{site}/Assets/{truck['id']}"

        payload = {
            "occurredOn": "2024-02-16T15:24:00",
            "properties": {
                "id": truck['id']
            },
            "related": {
                "AssetMeters": [
                    {
                        "properties": {
                            "meterId": {
                                "entity": "Meters",
                                "id": meter_type_id,
                                "isDeleted": False,
                                "number": 5,
                                "subtitle": {
                                    "id": 2,
                                    "subtitle": 2,
                                    "title": "Running"
                                },
                                "title": "Miles"
                            },        
                            "description": "Odometer " + truck['description'],
                            "currentValue": truck["odometerValue"],
                            "tempId": 1
                        },
                        "related": {
                            "AssetMeterReadings": []
                        },
                        "deleted": False
                    }
                ],
                "AssetParts": [],
            },
            "deleted": False
        }

        response_for_adding = requests.put(fluke, headers=headers, data=json.dumps(payload))

        if response_for_adding.status_code == 200 or response_for_adding.status_code == 201:
            # print(f"{truck['description']}: Odometer added successfully!")
            pass
        else:
            print(f"Failed making odometer. Status code: {response_for_adding.status_code}. Truck: {truck['description']}")
            # print("Error:", response_for_adding.text)
            # print(response_for_adding)

    # If there is only one, it will be read as a dictionary and give on the keys
    # It must always give the dictionary back, so if length is one it is converted to list and the one item is read as a dictionary
    oneNewTruck = False
    try:
        if trucks['id']:
            oneNewTruck=True
    except Exception as e:
        pass

    if oneNewTruck: 
        UploadingOdometerMeter(trucks)
    else:
        for truck in trucks:
            UploadingOdometerMeter(truck)

if __name__ == "__main__":
    # Could multithread next two lines - first does not take anytime at all though

    # Gets all the odometer values with associated truck
    motiveOdometers = getMotiveOdometerValues()
    # Gets all of the fluke truck and meter data
    trucks = getAllTruckAssets()

    # Inserts the odometer values into the truck data
    trucksAndOdometer = insertOdometerValues(motiveOdometers, trucks)

    # Updates the truck meter information
    info = getAllMeterInfos(trucksAndOdometer)
    freightliner_info = {
        "relatedInfo": info[0],
        "withoutMeterId": info[1],
        "withoutOdometer": info[2],
    }

    # Freightliners with an odometer and meter Id have the updated value pushed
    UpdateOdometerReadings(freightliner_info["relatedInfo"])
    MakeOdometerMeter(freightliner_info["withoutMeterId"])

    # Need to find out what to do with trucks without Odometers

