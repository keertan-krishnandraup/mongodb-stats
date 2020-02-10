from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from pprint import pprint

def get_stats(coll_name):
    client = MongoClient()
    stat_db = client['stats']
    stats_coll = stat_db['stats_coll']
    for i in list(stats_coll.find({})):
        if(list(i.keys())[1]==coll_name):
            return i[coll_name]

def plot_week(coll_name):
    dates = []
    insert_vals = []
    coll_stat = get_stats(coll_name)
    for j in coll_stat:
        dates.append(list(j.keys())[0])
        insert_vals.append(list(j.values())[0])
    x = pd.DataFrame(list(zip(dates, insert_vals)), columns=['Dates', 'Insert vals'])
    print(x)
    plot = x.plot(figsize = (16,16), x = 'Dates', y = 'Insert vals')
    #plt.xlabel(dates)
    #plt.xticks(rotation=45)
    plt.show()

def describe_coll(coll_name):
    dates = []
    insert_vals = []
    coll_stat = get_stats(coll_name)
    for j in coll_stat:
        dates.append(list(j.keys())[0])
        insert_vals.append(list(j.values())[0])
    x = pd.Series(insert_vals)
    print(x.describe(include='all'))
    #print('Max:',max(x),' Date:',index(max()))



if __name__=='__main__':
    print("Enter Coll Name:")
    coll_name = input()
    plot_week(coll_name)
    describe_coll(coll_name)