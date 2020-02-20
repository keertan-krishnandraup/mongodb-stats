"""using multiprocessing module, and placing the collection names in a queue, which are removed by a process and executed"""
import multiprocessing
from datetime import date, timedelta, datetime
import logging
import os
from pymongo import MongoClient
import time
x = multiprocessing.Queue()
PROCESSES = 200
#Mastering concurrency in Python
uri='mongodb://draupreader:fqp6hf9DzFMvLRaN@mongo-arbiter-harvestor.draup.technology:27017,mongodb-harvestor.draup.' \
    'technology:27017,mongodb1-harvestor.draup.technology:27017,mongodb2-harvestor.draup.technology:27017/admin?' \
    'replicaSet=draup-atlas-harvestor-replica-set&readPreference=primary'
logging.basicConfig(filename = 'stats_log.txt', level = logging.DEBUG, filemode = 'w')
num_days = 7
mill_day = 86400000
debug = 0

try:
    client = MongoClient(uri)
    logging.info(f"Connection to URI {uri} successful")
except ConnectionError as ce:
    logging.error(f"Connection to URI{uri} refused")
harvests_db = client['harvests']

processes = []

def pop_queue(cqueue, refresh):
    if (refresh == 1):
        coll_list = harvests_db.list_collection_names()
    else:
        coll_list = []
        f = open('coll_subset.json')
        all_colls = json.loads(f.read())
        for i in list(all_colls.keys()):
            if (all_colls[i] == 1):
                coll_list.append(i)
    print(coll_list)
    for i in coll_list:
        cqueue.put(i)
    print(cqueue.qsize())
    return cqueue

def get_stats_q(cqueue):
    local_client = MongoClient()
    stats_db = local_client['stats_processing']
    stats_coll = stats_db['stats_coll']
    pipeline = [{'$sort': {'_id': -1}},
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
    if(cqueue.empty()):
        print('empty')
    while not cqueue.empty():
        coll_name = cqueue.get(block=True,timeout=4)
        while not coll_name:
            coll_name = cqueue.get(block=True, timeout=4)
        try:
            res = list(harvests_db[coll_name].aggregate(pipeline))
        except Exception as exc:
            print('Exception')
            logging.error(exc)
            pass
        print(res)
        coll_stats = []
        if (not res):
            for k in date_buckets:
                coll_stats.append({k.strftime("%d-%b-%Y (%H:%M:%S)"): 0})
        else:
            #if (debug):
                #f = open('write.txt', 'a')
                #f.write(str(res))
            res_idx = 0
            for j in date_buckets:
                if (res_idx < len(res) and res[res_idx]['_id'] == j):
                    coll_stats.append({j.strftime("%d-%b-%Y (%H:%M:%S)"): res[res_idx]['count']})
                    res_idx += 1
                else:
                    coll_stats.append({j.strftime("%d-%b-%Y (%H:%M:%S)"): 0})
        print('coll:',coll_stats)
        coll_dict = {coll_name: coll_stats}
        #Acquire Lock?
        stats_coll.insert_one(coll_dict)
    return 1


def run():
    empty_queue = multiprocessing.Queue()
    #print(empty_queue)
    full_queue = pop_queue(empty_queue, 1)
    print(full_queue.qsize())
    processes = []
    print(f'Running with {PROCESSES} processes')
    for num in range(PROCESSES):
        p = multiprocessing.Process(target=get_stats_q,args=(full_queue,))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()

if __name__=='__main__':
    start = time.time()
    run()
    end = time.time()
    print(end - start, ' seconds')