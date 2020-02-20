"""Splitting up processing of collections, across threads using the threading module"""
"""Explore relationship w/ number of threads and time taken"""

from pymongo import MongoClient
from datetime import date, timedelta, datetime
from bson.objectid import ObjectId
from pprint import pprint
import logging
import time
import json
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
#Mastering concurrency in Python
uri='mongodb://draupreader:fqp6hf9DzFMvLRaN@mongo-arbiter-harvestor.draup.technology:27017,mongodb-harvestor.draup.' \
    'technology:27017,mongodb1-harvestor.draup.technology:27017,mongodb2-harvestor.draup.technology:27017/admin?' \
    'replicaSet=draup-atlas-harvestor-replica-set&readPreference=primary'
logging.basicConfig(filename = 'stats_log.txt', level = logging.DEBUG, filemode = 'w')
num_days = 7
mill_day = 86400000
debug = 0

class coll_entry():
    def __init__(self, name):
        self.name = name
        self.picked = False
    def get_name(self):
        return self.name
    def get_picked(self):
        return self.picked

coll_queue = Queue(maxsize = 0)
threads = []
try:
    client = MongoClient(uri)
    logging.info(f"Connection to URI {uri} successful")
except ConnectionError as ce:
    logging.error(f"Connection to URI{uri} refused")
harvests_db = client['harvests']



def pop_queue_mt2(cqueue, refresh):
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

def thread_execute(cqueue, num_threads):
    for i in range(num_threads):
        worker = Thread(target=get_stats_t2, args=(cqueue,))
        worker.setDaemon(True)
        worker.start()
        threads.append(worker)

def get_stats_t2(cqueue):
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
    while True:
        coll_name = cqueue.get(block=True,timeout=4)
        if coll_name is None:
            break
        try:
            res = list(harvests_db[coll_name].aggregate(pipeline))
        except Exception as exc:
            print('Exception')
            logging.error(exc)
            pass
        #print(res)
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
        #print('coll:',coll_stats)
        coll_dict = {coll_name:coll_stats}
        #Acquire Lock?
        local_client = MongoClient()
        stats_db = local_client['stats_threading']
        stats_coll = stats_db['stats_coll2']
        stats_coll.insert_one(coll_dict)
        cqueue.task_done()

def wait(cqueue,threads):
    #print('waitinggg')
    cqueue.join()
    for i in range(len(threads)):
        cqueue.put(None)
    for t in threads:
        t.join()

def mthreading_stats2():
    num_threads = 500
    pop_queue_mt2(coll_queue, 1)
    thread_execute(coll_queue, num_threads)
    wait(coll_queue, threads)

if __name__=='__main__':
    start = time.time()
    mthreading_stats2()
    end = time.time()
    print(end - start, ' seconds')


