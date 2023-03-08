from pyspark import StorageLevel, RDD
import time
from statistics import variance

def question3(rdd: RDD):
    start = time.perf_counter()

    variance410 = rdd.cartesian(rdd)\
    .filter(lambda pair: pair[0][0] < pair[1][0])\
    .map(lambda pair: [pair[0][0]] + [pair[1][0]] + [x+y for x, y in zip(pair[0][1:], pair[1][1:])])\
    .cartesian(rdd)\
    .filter(lambda pair: pair[0][1] < pair[1][0])\
    .map(lambda pair: ((pair[0][0], pair[0][1], pair[1][0]), variance([x+y for x, y in zip(pair[0][2:], pair[1][1:])])))\
    .filter(lambda pair: pair[1] <= 410)\
    .persist(StorageLevel.MEMORY_ONLY)

    variance20 = variance410.filter(lambda pair: pair[1] <= 20).persist(StorageLevel.MEMORY_ONLY)      
    t20 = variance20.count()
    triples20 = variance20.map(lambda pair: '-'.join(map(str, pair[0]))).collect()
    variance20.unpersist()
    t410 = variance410.count()
    variance410.unpersist()

    print(f"τ=20 triples: {triples20}")
    print(f"τ=20: {t20}")
    print(f"τ=410: {t410}")

    print(f"seconds to calculate: {time.perf_counter() - start:0.2f}")