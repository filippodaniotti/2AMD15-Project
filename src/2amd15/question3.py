import time
import numpy as np
from pyspark import StorageLevel, RDD
from statistics import variance
#_END_IMPORTS


#_BEGIN_CODE
def question3(rdd: RDD):
    print('>> executing question 3')
    start = time.perf_counter()

    variance410 = rdd.repartition(50)\
        .cartesian(rdd)\
        .filter(lambda pair: pair[0][0] < pair[1][0])
    print(">> partitions after first cartesian: ", variance410.getNumPartitions())

    variance410 = variance410.repartition(2000)\
        .map(lambda pair:
             [pair[0][0]]
             + [pair[1][0]]
             + [pair[0][1] + pair[1][1]])\
        .cartesian(rdd)\
        .filter(lambda pair: pair[0][1] < pair[1][0])

    print(">> partitions after repartition + second cartesian: ", variance410.getNumPartitions())
    variance410 = variance410\
        .map(lambda pair: (
            (pair[0][0], pair[0][1], pair[1][0]),
            np.var(pair[0][2] + pair[1][1], ddof=0)
        ))

    print(">> partitions after map and filter: ", variance410.getNumPartitions())

    variance410 = variance410.filter(lambda pair: pair[1] <= 410)\
        .persist(StorageLevel.MEMORY_AND_DISK)

    triples20 = variance410.filter(lambda pair: pair[1] <= 20)\
        .map(lambda pair: '-'.join(map(str, pair[0])))\
        .collect()
    t20 = len(triples20)
    t410 = variance410.count()
    variance410.unpersist()

    print(f">> τ=20 triples: {triples20}")
    print(f">> τ=20: {t20}")
    print(f">> τ=410: {t410}")

    print(f">> seconds to calculate: {time.perf_counter() - start:0.2f}")

# 25_- = 67s
# 250_250 = 118
# 25_250 = 66
# 10_- = 59
# 10_500 = 70
