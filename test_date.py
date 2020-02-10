from pymongo import MongoClient
from datetime import date, timedelta, datetime
from bson.objectid import ObjectId
client = MongoClient()
db = client['test']
print(db.list_collection_names())
for i in list(db.list_collection_names()):
    print(db[i].find_one({}))
persons = db['persons']
mill_day = 86400000
mil_sec = datetime.timestamp(datetime.now())-3*mill_day/1000
#/1000
print(datetime.fromtimestamp(mil_sec).date())
#one_pers = persons.find_one({'index':0})
#sec_pers = persons.find_one({'index':1})
#print(str((sec_pers['_id'].generation_time-one_pers['_id'].generation_time).days) + ' days')
