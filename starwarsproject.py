import requests
import pymongo
import pprint

pp=pprint.PrettyPrinter()

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


## Function to return a dictionary of pilots : URL -> Pilot Name pairs
def generate_pilot_dict(ships):
    pdict={}
    for s in ships:
        if s.get("pilots"):
            for p in s.get("pilots"):
                if p not in pdict:  ### If this pilot has not already been fetched then go call the api
                    name = get_pilot_name(p)
                    pdict[p] = name
    return pdict


shiplist = get_ships_list()
print(f"LENGTH = {len(shiplist)}")

pilot_dict = generate_pilot_dict(shiplist)
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
for s in shiplist:
    new_pilot_list=[]
    for p in s["pilots"]:
        pname=pilot_dict.get(p)
        objid=pilotobj_dict.get(pname)
        new_pilot_list.append(objid)
    s["pilots"] = new_pilot_list

print ("--- SHIPS LIST---")
pp.pprint(shiplist)

## IF collection has already existed we want to clear it out
db.drop_collection("starships")

### Now write the newly-updated starships collection into mongo
db.starships.insert_many(shiplist)

print ("--- SAVED TO MONGO ---")

### Close the mongo connection
client.close()