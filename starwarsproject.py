import requests
import pymongo
import pprint

pp=pprint.PrettyPrinter()

### Func that returns a dict from api
def get_data(query):
    resp = requests.get(query)
    return resp.json()

### Func to get all starships handling paging
def get_ships_list():
    qry_ships="https://swapi.dev/api/starships"
    bDone=False   # setting boolean variable for while loop
    shipslist=[]    # creating an empty list which we will add to below

    while not bDone:
        shipsdict = get_data(qry_ships)
        if "next" in shipsdict and shipsdict["next"] is not None: # this will check multiple pages after initial page and then resets the url to the next page of ships
            qry_ships=shipsdict["next"]
        else:
            bDone=True
        shipslist=shipslist+shipsdict["results"]

    return shipslist

### Func that retuns pilot name
def get_pilot_name(url):
    pil = get_data(url)
    return pil.get("name")


### Function to return a dictionary of pilots : URL -> Pilot Name pairs
def generate_pilot_dict(ships):
    pdict={}
    for s in ships:
        for p in s.get("pilots"):
            if p not in pdict:  ### If this pilot has not already been fetched then go call the api
                name = get_pilot_name(p) ### using function to get th pilot name
                pdict[p] = name ### adding pilot name to pilot dictionary
    return pdict

### Go get all the ships
shiplist = get_ships_list() ### assigning global variable shiplist to call for future functions
print(f"LENGTH = {len(shiplist)}")  ### checking to see if count of ships matches

### get a dictionary with the pilot url key and matching name values from people api
pilot_dict = generate_pilot_dict(shiplist)
print(pilot_dict)

### Now go search for each pilot in mongo and get the ObjectIDs
client = pymongo.MongoClient()
db = client['starwars']

### Use another key-value dictionary for looking up the name to the objectid
pilotobj_dict={}

for pname in pilot_dict.values():                           ## the values gives me the list of pilot names
    if pname not in pilotobj_dict:                          ## Skip any pilots we've looked up here before
        pdoc = db.characters.find_one({"name": pname})      ## Go search the data collection for this plot by name
        pilotobj_dict[pname]=pdoc.get("_id")                ## Get the Object Id for the pilot

print ("--- PILOTOBJ DICT---")
print (pilotobj_dict)

### Now we overwrite the ships list so that each ship's pilot array has the object ids.
for s in shiplist:
    new_pilot_list=[]
    for p in s["pilots"]:                       # Iterate over the pilots list for this ship
        pname=pilot_dict.get(p)                 # Get the pilot's name from the lookup dict
        objid=pilotobj_dict.get(pname)          # Using the name, lookup the obj id from the pilotobj dict
        new_pilot_list.append(objid)            # Append the OBjectID to the list
    s["pilots"] = new_pilot_list                # Overwrite this ship's Pilot's list with the new list of ObjectIDs

print ("--- SHIPS LIST---")
pp.pprint(shiplist)

### IF collection has already existed we want to clear it out
db.drop_collection("starships")

### Now write the newly-updated starships collection into mongo
db.starships.insert_many(shiplist)

print ("--- SAVED TO MONGO ---")

### Close the mongo connection
client.close()