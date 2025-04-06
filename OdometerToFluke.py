# Imports
import json
import requests
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime
import math
from zoneinfo import ZoneInfo


production = True

# config
tenant = "torcrobotics.us.accelix.com" if production else "torcroboticssb.us.accelix.com"
site = "def"

# Cookie to the sandbox
sandbox_key = os.getenv("SANDBOX_KEY")
production_key = os.getenv("PRODUCTION_KEY")
motive_key = os.getenv("MOTIVE_KEY")

headers = {'Content-Type': 'application/json', 'Cookie': production_key if production else sandbox_key}

def getMotiveOdometerValues():

    # Get How many pages contain all of the data
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
            odometer_readings.append([cursor['number'], cursor['current_location']['odometer'], {"lat": cursor['current_location']['lat'], "lon": cursor['current_location']['lon']}])
        else:
            print(f"{cursor['number']} has no odometer reading")

    return odometer_readings

# Gets all of the truck ids from fluke
def getAllTruckAssets():
    assets_url = f'https://{tenant}/api/entities/{site}/Assets/search-paged?includeRelated=true'

    data = {
        "select": [
            {"name": "c_serialnumber"},
            {"name": "number"},
            {"name": "c_description"},
            {"name": "c_assettype"}
        ],
        "filter": {
            "and": [
                {"name": "isDeleted", "op": "isfalse"},
                {"name": "c_assettype", "op": "eq", "value": "Freightliner"}
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

    print("Getting all freightliner ids")

    for page in range(1, pages):
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
def updateOdometerValues(odometer_data, asset_data):
    def km_to_mile(km):
        return round(km * 0.621371, 2)

    def getRelatedInfo(asset: dict):
        assetId = asset['id']

        # Get the related information of the asset for its odometer value
        relatedInfoFluke = f"https://{tenant}/api/entities/{site}/Assets/{assetId}?includeRelated=true" # Include related to get odometer field information
        asset_response = requests.get(relatedInfoFluke, headers=headers)
        response = json.loads(asset_response.text)

        # Has an asset meter
        if not (response['related']['AssetMeters'] == None or len(response['related']['AssetMeters']) == 0):
            # Make sure the odometer has moved
            currentOdometerValue = response['related']['AssetMeters'][0]['properties']['currentValue']
            if currentOdometerValue == asset['odometer_value']:
                print(asset['truck_from_motive'] + " has not updated position.")
                return


            necessary_info = {
                "id": response['properties']['id'],
                "description": asset['truck_from_motive'],
                'meterId': response['related']['AssetMeters'][0]['properties']['id'],
                'odometerValue': asset['odometer_value'],
                'positionValue': asset['position'],
                'related': response['related'],
                'hasMeter': True
            }
            
            print(response['properties']['c_description'] + ": Ready to go")

            return necessary_info

        # Does not have an asset meter
        else:
            necessary_info = {
                "id": response['properties']['id'],
                "description": asset['truck_from_motive'],
                'odometerValue': asset['odometer_value'],
                'positionValue': asset['position'],
                'hasMeter': False
            }

            print(response['properties']['c_description'] + ": No meter")

            return necessary_info

    def addAssetMeterReading(truck):
        # ADD ASSET METER READING
        assetMeterReadings = f'https://{tenant}/api/entities/{site}/AssetMeterReadings'

        payload = {
            "properties": {
                "date": str(datetime.now(ZoneInfo("America/Chicago"))),
                "assetMeterId": truck['meterId'],
                "value": truck['odometerValue']
            },
            "related": {},
            "deleted": False
        }

        print(payload)
        
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

        # Add location of truck
        updateTruckLocation = f'https://{tenant}/api/entities/{site}/Assets/{truck["id"]}'

        payload = {
            "properties": {
                "geolocation": {
                    "lat": truck['positionValue']['lat'],
                    "long": truck['positionValue']['lon'],
                },
                "id": truck['id']
            }
        }

        print(payload)

        response = requests.put(url=updateTruckLocation, headers=headers, data=json.dumps(payload))

        try: 
            # Check response
            if response.status_code == 200:
                print(truck['description'] + ": Location updated successfully!")
                print(response.json())
            else:
                print(f"Failed. Status code: {response.status_code}")
                print("Error:", response.text)
                print(response)
        except Exception as e:
            pass

    def UploadingOdometerMeter(truck):
        fluke = f"https://{tenant}/api/entities/{site}/Assets/{truck['id']}"

        # The type of meter that is being created
        meter_type_id = "6330cf04-5555-44b7-aad8-a843d9e438d1"

        payload = {
            "occurredOn": datetime.now().isoformat(),
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
                            "description": "Odometer",
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

        print(payload)

        response_for_adding = requests.put(fluke, headers=headers, data=json.dumps(payload))

        try: 
            if response_for_adding.status_code == 200 or response_for_adding.status_code == 201:
                print(f"{truck['description']}: Odometer added successfully!")
            else:
                print(f"Failed. Status code: {response_for_adding.status_code}")
                print("Error:", response_for_adding.text)
                print(response_for_adding)
        except Exception:
            print("Failed to upload truck")

    # Iterate through the odometer data and match with asset dictionary
    for adding_info in odometer_data:

        key = adding_info[0] # The Truck Number
        odometer_val = adding_info[1] # The odometer
        position_val = adding_info[2]
        
        assetKey = key.split(' ')[0]  # Extract the first part of the key
        
        for asset in asset_data:
            truckName = asset['c_description']

            # When a truck from motive is matched with one from fluke
            if assetKey in truckName:
                
                if odometer_val != "N/A" and odometer_val != None:
                    asset['odometer_value'] = km_to_mile(odometer_val)  # Add odometer value
                    asset['truck_from_motive'] = assetKey
                    asset['position'] = position_val

                    necessaryInfo = getRelatedInfo(asset)

                    # nothing to change on fluke - no new odometer value
                    if not necessaryInfo:
                        continue
                    
                    # Has an asset meter that can be updated
                    if(necessaryInfo['hasMeter']):
                        print(asset['truck_from_motive'] + " moved position")

                        addAssetMeterReading(necessaryInfo)
                    # Needs an asset meter created
                    else:
                        print(asset['truck_from_motive'] + " needs an asset meter")

                        UploadingOdometerMeter(necessaryInfo)

if __name__ == "__main__":
    # Could multithread next two lines - first does not take anytime at all though

    # Gets all the odometer values with associated truck
    motiveOdometers = getMotiveOdometerValues()
    # Gets all of the fluke truck and meter data
    trucks = getAllTruckAssets()

    # Inserts the odometer values into the truck data
    # update the truck meter information
    # create truck asset meter if needed
    inserted_odometers = updateOdometerValues(motiveOdometers, trucks)
