import requests
import json
import pymongo
import pprint

pp=pprint.PrettyPrinter(indent=2)

def get_data(query):
    resp = requests.get(query)
    return resp.json()

###
def get_ships_list():
    qry_ships="https://swapi.dev/api/starships"
    bDone=False
    shipslist=[]

    while not bDone:
        dd=get_data(qry_ships)
        if "next" in dd and dd["next"] is not None:
            qry_ships=dd["next"]
        else:
            bDone=True
        shipslist=shipslist+dd["results"]

    return shipslist

####
def get_pilot_name(url):
    pil = get_data(url)
    if "name" in pil:
        return pil["name"]
    else:
        return None

ships=get_ships_list()
print(f"LENGTH = {len(ships)}")

pilot_dict={}

## Next to get all the pilots names by iterating over all the pilots in the ships list
for s in ships:
    if s["pilots"]:
        for p in s["pilots"]:
            if p not in pilot_dict:   ### If this pilot has not already been fetched then go call the api
                name=get_pilot_name(p)
                pilot_dict[name]=p

print(pilot_dict)

## Now go search for each pilot in mongo and get the ObjectIDs
