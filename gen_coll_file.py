import json
from pymongo import MongoClient
import json
uri='mongodb://draupreader:fqp6hf9DzFMvLRaN@mongo-arbiter-harvestor.draup.technology:27017,mongodb-harvestor.draup.' \
    'technology:27017,mongodb1-harvestor.draup.technology:27017,mongodb2-harvestor.draup.technology:27017/admin?' \
    'replicaSet=draup-atlas-harvestor-replica-set&readPreference=primary'
client = MongoClient(uri)
db = client['harvests']
coll_list = db.list_collection_names()
tmp_dict = dict()
for i in coll_list:
    tmp_dict[i] = 0
x = json.dumps(tmp_dict, indent = 4)
f = open('coll_subset.json', 'w')
f.write(x)
f.close()