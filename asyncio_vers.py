"""Uses asyncio library, and makes one task per collection"""
from pymongo import MongoClient
from datetime import date, timedelta, datetime
from bson.objectid import ObjectId
from pprint import pprint
import logging
import time
import json
import asyncio
import motor.motor_asyncio
import json
#Mastering concurrency in Python
uri='mongodb://draupreader:fqp6hf9DzFMvLRaN@mongo-arbiter-harvestor.draup.technology:27017,mongodb-harvestor.draup.' \
    'technology:27017,mongodb1-harvestor.draup.technology:27017,mongodb2-harvestor.draup.technology:27017/admin?' \
    'replicaSet=draup-atlas-harvestor-replica-set&readPreference=primary'
logging.basicConfig(filename = 'stats_log.txt', level = logging.DEBUG, filemode = 'w')
num_days = 7
mill_day = 86400000
debug = 0

coll_queue = asyncio.Queue(maxsize = 0)
try:
    client = motor.motor_asyncio.AsyncIOMotorClient(uri)
    logging.info(f"Connection to URI {uri} successful")
except ConnectionError as ce:
    logging.error(f"Connection to URI{uri} refused")
harvests_db = client.get_database('harvests')

async def pop_queue_async(cqueue, refresh = 1):
    if(refresh==1):
        coll_list = await harvests_db.list_collection_names()
    else:
        coll_list = []
        f = open('coll_subset.json')
        all_colls = json.loads(f.read())
        for i in list(all_colls.keys()):
            if(all_colls[i]==1):
                coll_list.append(i)
    print(coll_list)
    for i in coll_list:
        await cqueue.put(i)
    print(cqueue.qsize())

async def asyncio_execute(cqueue):
    tasks = []
    for i in range(cqueue.qsize()):
        task = asyncio.Task(get_stats_async(cqueue))
        tasks.append(task)
    await asyncio.gather(*tasks)


async def get_stats_async(cqueue):

    pipeline = [{'$sort': {'_id': 1}},
                {'$limit': 50},
                {'$project': {'_id': '$_id', 'convDate': {'$toDate': "$_id"}}},
                {'$project': {'_id': 1, 'createDate': '$convDate', 'diff': {'$subtract': [
                    datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1),
                    '$convDate']}}},
                {'$match': {'diff': {'$lt': (num_days + 1) * mill_day}}},
                {'$bucket': {'groupBy': "$createDate", 'boundaries': [
                    datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=i) for i in
                    range(7, -2, -1)], 'default': "Other",
                             'output': {'createDate': {'$push': '$createDate'}, 'count': {'$sum': 1},
                                        'diff': {'$push': '$diff'}}}}
                ]
    date_buckets = [datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=i) for i in
                    range(7, -1, -1)]
    coll_name = await cqueue.get()
    if(not coll_name):
        return
    #coll_stats
    res_list = []
    try:
        async for doc in harvests_db[coll_name].aggregate(pipeline):
            res_list.append(doc)
    except Exception as exc:
        print('Exception')
        logging.error(exc)
        pass
    #print(res_list)
    coll_stats = []
    if (not res_list):
        for k in date_buckets:
            coll_stats.append({k.strftime("%d-%b-%Y (%H:%M:%S)"): 0})
    else:
        # if (debug):
        # f = open('write.txt', 'a')
        # f.write(str(res))
        res_idx = 0
        for j in date_buckets:
            if (res_idx < len(res_list) and res_list[res_idx]['_id'] == j):
                coll_stats.append({j.strftime("%d-%b-%Y (%H:%M:%S)"): res_list[res_idx]['count']})
                res_idx += 1
            else:
                coll_stats.append({j.strftime("%d-%b-%Y (%H:%M:%S)"): 0})
    coll_dict = {coll_name: coll_stats}
    #print('coll:', coll_stats)
    #Acquire Lock?
    local_client = motor.motor_asyncio.AsyncIOMotorClient()
    stats_db = local_client['stats_asyncio']
    stats_coll = stats_db['stats_coll']
    await stats_coll.insert_one(coll_dict)

async def driver(cqueue, refresh):
    start = time.time()
    await pop_queue_async(coll_queue, refresh)
    end = time.time()
    print(end-start, 'seconds: Populating stuff')
    start= time.time()
    await asyncio_execute(coll_queue)
    end = time.time()
    print(end-start, 'seconds: Execution')

def asyncio_stats(refresh=1):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(driver(coll_queue, refresh))


if __name__=='__main__':
    start = time.time()
    print('Would you like to refresh? y/n')
    choice = input()
    if(choice=='y'):
        refresh = 1
    else:
        refresh = 0
    asyncio_stats(refresh)
    end = time.time()
    print(end - start, ' seconds: Total Execution')


