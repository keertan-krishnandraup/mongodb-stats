from pymongo import MongoClient
from datetime import date,timedelta,datetime
from bson.objectid import ObjectId
from pprint import pprint
import logging
import time
import json
#Mastering concurrency in Python
uri='mongodb://draupreader:fqp6hf9DzFMvLRaN@mongo-arbiter-harvestor.draup.technology:27017,mongodb-harvestor.draup.' \
    'technology:27017,mongodb1-harvestor.draup.technology:27017,mongodb2-harvestor.draup.technology:27017/admin?' \
    'replicaSet=draup-atlas-harvestor-replica-set&readPreference=primary'
logging.basicConfig(filename = 'stats_log.txt', level = logging.DEBUG, filemode = 'w')
num_days = 7
mill_day = 86400000
debug = 0

def brute_stats():
    try:
        client = MongoClient(uri)
        logging.info(f"Connection to URI {uri} successful")
    except ConnectionError as ce:
        logging.error(f"Connection to URI{uri} refused")
    harvests_db = client['harvests']
    coll_list = list(harvests_db.list_collection_names())
    pipeline = [{'$sort':{'_id':-1}},
                {'$limit': 50},
                {'$project':{'_id':'$_id','convDate':{'$toDate':"$_id"}}},
                {'$project':{'_id':1,'createDate':'$convDate','diff':{'$subtract':[datetime.now().replace(hour=0, minute = 0, second = 0, microsecond = 0)+timedelta(days=1),'$convDate']}}},
                {'$match':{'diff':{'$lt':(num_days+1)*mill_day}}},
                {'$bucket':{'groupBy':"$createDate",'boundaries':[datetime.now().replace(hour=0, minute = 0, second = 0, microsecond = 0)-timedelta(days=i) for i in range(7,-2,-1)],'default':"Other",'output':{'createDate':{'$push':'$createDate'},'count':{'$sum':1},'diff':{'$push':'$diff'}}}}
            ]
    #Change this for actual range
    coll_range = (i for i in range(0,len(coll_list)))
    stats = []
    date_buckets = [datetime.now().replace(hour=0, minute = 0, second = 0, microsecond = 0)-timedelta(days=i) for i in range(7,-1,-1)]
    for i in coll_range:
        res = list(harvests_db[coll_list[i]].aggregate(pipeline))
        coll_stats = []
        if(not res):
            for k in date_buckets:
                coll_stats.append({k.strftime("%d-%b-%Y (%H:%M:%S)"): 0})
        else:
            if(debug):
                f = open('write.txt','a')
                f.write(str(res))
            res_idx = 0
            for j in date_buckets:
                if(res_idx<len(res) and res[res_idx]['_id']==j):
                    coll_stats.append({j.strftime("%d-%b-%Y (%H:%M:%S)"):res[res_idx]['count']})
                    res_idx +=1
                else:
                    coll_stats.append({j.strftime("%d-%b-%Y (%H:%M:%S)"):0})
        stats.append({coll_list[i]:coll_stats})
    if(debug):
        f = open('final.txt','w')
        f.write(json.dumps(stats, indent = 4))
    local_client = MongoClient()
    stats_db = local_client['stats']
    stats_db.stats_coll.drop()
    stats_coll = stats_db['stats_coll']
    stats_coll.insert_many(stats)


if __name__=='__main__':
    start = time.time()
    brute_stats()
    end = time.time()
    print(end-start, ' seconds')