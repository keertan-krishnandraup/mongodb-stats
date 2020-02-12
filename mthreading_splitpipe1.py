"""Splitting up processing of collections, across threads using the concurrent.futures class using ThreadPoolExecutors"""
from pymongo import MongoClient
from datetime import date, timedelta, datetime
from bson.objectid import ObjectId
from pprint import pprint
import logging
import time
import json
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
#Mastering concurrency in Python
uri='mongodb://draupreader:fqp6hf9DzFMvLRaN@mongo-arbiter-harvestor.draup.technology:27017,mongodb-harvestor.draup.' \
    'technology:27017,mongodb1-harvestor.draup.technology:27017,mongodb2-harvestor.draup.technology:27017/admin?' \
    'replicaSet=draup-atlas-harvestor-replica-set&readPreference=primary'
logging.basicConfig(filename = 'stats_log.txt', level = logging.DEBUG, filemode = 'w')
num_days = 7
mill_day = 86400000
debug = 0

coll_queue1 = Queue(maxsize = 0)
coll_queue2 = Queue(maxsize = 0)
coll_queue3 = Queue(maxsize = 0)
coll_queue4 = Queue(maxsize = 0)
coll_queue5 = Queue(maxsize = 0)
coll_queue6 = Queue(maxsize = 0)
t_pool1 = ThreadPoolExecutor(8)
t_pool2 = ThreadPoolExecutor(8)
t_pool3 = ThreadPoolExecutor(8)
t_pool4 = ThreadPoolExecutor(8)
t_pool5 = ThreadPoolExecutor(8)
t_pool6 = ThreadPoolExecutor(8)



try:
    client = MongoClient(uri)
    logging.info(f"Connection to URI {uri} successful")
except ConnectionError as ce:
    logging.error(f"Connection to URI{uri} refused")
harvests_db = client['harvests']


def pop_queue(cqueue):
    coll_list = list(harvests_db.list_collection_names())
    for i in coll_list:
        #x = coll_entry(i)
        cqueue.put(i)


def pop_queue_and_limit(cqueue, lim):
    coll_list = list(harvests_db.list_collection_names())
    for i in coll_list:
        docs = harvests_db[i].find().limit(int(lim))#sort({'_id':-1})#.limit(int(lim))
        print(len(list(docs)))
        for j in docs:
            cqueue.put(j)

def sort_and_limit_stage(cqueue_in,cqueue_out):
    stop_flag = 0
    stage_pipe = [{'$sort': {'_id': -1}},
                  {'$limit': 50}]
    while(stop_flag != 1):
        coll_name = cqueue_in.get(block = True, timeout = 2)
        while not coll_name:
            coll_name = cqueue_in.get(block=True, timeout=2)
        res = list(harvests_db[coll_name].aggregate(stage_pipe))
        cqueue_out.put(tuple(coll_name:res))

def project1_stage(cqueue_in,cqueue_out):
    stop_flag = 0
    stage_pipe = [{'$project': {'_id': '$_id', 'convDate': {'$toDate': "$_id"}}}]
    while (stop_flag != 1):
        obj = cqueue_in.get(block=True, timeout=2)
        while not obj:
            obj = cqueue_in.get(block=True, timeout=2)
        coll_name = obj[0]
        obj_list = obj[1]
        for i in
        res = list(harvests_db[coll_name].aggregate(stage_pipe))
        cqueue_out.put(tuple(coll_name: res))

def project2_stage():
    pass

def match_stage():
    pass

def bucket_stage():
    pass

def stage_execute(cqueue,n,fn_name):
    with ThreadPoolExecutor(max_workers=n) as executor:
        q_len = cqueue.qsize()
        futures = {executor.submit(fn_name, cqueue):i for i in range(q_len)}
        for future in as_completed(futures):
            name = futures[future]
            try:
                data = future.result()
                #print(data)
            except Exception as exc:
                print(exc)





def get_stats_q(cqueue):
    pipeline = [

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
    while not cqueue.empty():
        coll_name = cqueue.get(block=True,timeout=4)
        while not coll_name:
            coll_name = cqueue.get(block=True, timeout=4)
        res = list(harvests_db[coll_name].aggregate(pipeline))
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
        coll_dict = {coll_name: coll_stats}
        #Acquire Lock?
        local_client = MongoClient()
        stats_db = local_client['stats_threading']
        stats_coll = stats_db['stats_coll']
        stats_coll.insert_one(coll_dict)
        return 1

stage_dict = {
    1:sort_and_limit_stage,
    2:project1_stage,
    3:project2_stage,
    4:match_stage,
    5:bucket_stage
}


if __name__=='__main__':
    start = time.time()
    pop_queue(coll_queue1)
    #pool_execute(coll_queue,500)
    end = time.time()
    print(end - start, ' seconds')


