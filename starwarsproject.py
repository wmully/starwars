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
    if s.get("pilots"):
        for p in s.get("pilots"):
            if p not in pilot_dict:   ### If this pilot has not already been fetched then go call the api
                name=get_pilot_name(p)
                pilot_dict[p]=name

print(pilot_dict)

## Now go search for each pilot in mongo and get the ObjectIDs
client = pymongo.MongoClient()
db = client['starwars']

## Use another key-value dictionary gfor looking up the name to the objectid
pilotobj_dict={}

for pname in pilot_dict.values():
    pdoc=db.characters.find_one({"name": pname})
    if pname not in pilotobj_dict:
        pilotobj_dict[pname]=pdoc.get("_id")

print ("--- PILOTOBJ DICT---")
print (pilotobj_dict)

### Now we overwrite the ships list so that each ship's pilot array has the object ids.
for s in ships:
    new_pilot_list=[]
    for p in s["pilots"]:
        pname=pilot_dict.get(p)
        objid=pilotobj_dict.get(pname)
        new_pilot_list.append(objid)
    s["pilots"] = new_pilot_list

print ("--- SHIPS LIST---")
pp.pprint(ships)

### Now write the newly-updated starships collection into mongo
db.starships.insert_many(ships)

print ("--- SAVED TO MONGO ---")

### Close the mongo connection
client.close()