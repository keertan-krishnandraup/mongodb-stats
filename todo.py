"""Brute Force Approach, w/ no multithreading or multiprocessing
Multithreading
Multiprocessing
Asynchronous
Metrics:
Total number
Std. Deviation
Average per day
Maximum
Minimum OF
Count of documents
ON
combinations of collections of one particular DB
Combination of fields of one particular DB
"""

from scipy.stats import describe
import time
from datetime import datetime
print(datetime.now())
print(time.time())
print(list(describe([1,2,3,4,5])))