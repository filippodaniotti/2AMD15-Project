import time
import numpy as np
from pyspark import RDD
from statistics import pvariance, mean
#_END_IMPORTS


#_BEGIN_CODE
def question3(rdd: RDD):
    print('>> executing question 3')
    print('>> vector count ', rdd.count())
    print('>> vector length ', len(rdd.take(1)[0][1]))
    start = time.perf_counter()

    vectorMap = rdd.collectAsMap()
    vectorMapBroadcast = rdd.context.broadcast(vectorMap)

    t410 = rdd.context.accumulator(0)

    def calc_var(acc, broadcast):
        def inner(keys):
            var = np.var(
                broadcast.value[keys[0][0]]
                + broadcast.value[keys[0][1]]
                + broadcast.value[keys[1]]
            )

            if var <= 410:
                acc.add(1)
            return (
                (keys[0][0], keys[0][1], keys[1]),
                var
            )
        return inner

    rdd = rdd.keys().cache()

    triples20 = rdd\
        .cartesian(rdd)\
        .coalesce(80)\
        .filter(lambda pair: pair[0] < pair[1])\
        .cartesian(rdd)\
        .coalesce(160)\
        .filter(lambda pair: pair[0][1] < pair[1])\
        .map(calc_var(t410, vectorMapBroadcast))\
        .filter(lambda pair: pair[1] <= 20)\
        .map(lambda pair: '-'.join(map(str, pair[0])))\
        .collect()
    t20 = len(triples20)

    print(f">> τ=410: {t410.value}")
    print(f">> τ=20 triples: {triples20}")
    print(f">> τ=20: {t20}")

    print(f">> seconds to calculate: {time.perf_counter() - start:0.2f}")
    rdd.unpersist()
