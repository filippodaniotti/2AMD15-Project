import time
from pyspark import StorageLevel, RDD
from statistics import variance
#_END_IMPORTS


#_BEGIN_CODE
def question3(rdd: RDD):
    start = time.perf_counter()

    variance410 = rdd.cartesian(rdd)\
        .filter(lambda pair: pair[0][0] < pair[1][0])\
        .repartition(25)\
        .map(lambda pair:
             [pair[0][0]]
             + [pair[1][0]]
             + [x+y for x, y in zip(pair[0][1:], pair[1][1:])])\
        .cartesian(rdd)\
        .filter(lambda pair: pair[0][1] < pair[1][0])\
        .map(lambda pair: (
            (pair[0][0], pair[0][1], pair[1][0]),
            variance([x+y for x, y in zip(pair[0][2:], pair[1][1:])])
        ))\
        .filter(lambda pair: pair[1] <= 410)\
        .persist(StorageLevel.MEMORY_ONLY)

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
