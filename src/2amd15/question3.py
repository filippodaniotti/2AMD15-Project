import time
import numpy as np
from pyspark import RDD, SparkContext, Accumulator
from pyspark.broadcast import Broadcast

from typing import Callable, Tuple
#_END_IMPORTS


#_BEGIN_CODE
def question3(spark_context: SparkContext, rdd: RDD) -> None:
    print('>> executing question 3')
    print('>> vector count ', rdd.count())
    print('>> vector length ', len(rdd.take(1)[0][1]))
    start = time.perf_counter()

    vector_map = rdd.collectAsMap()
    vector_map_broadcast = spark_context.broadcast(vector_map)

    t410 = spark_context.accumulator(0)

    def calc_var(acc: Accumulator, broadcast: Broadcast) -> Callable:
        def inner(
                keys: Tuple[Tuple[str, str], str]
            ) -> Tuple[Tuple[str, str, str], np.float32]:
            
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

    triples20 = rdd \
        .cartesian(rdd) \
        .coalesce(128) \
        .filter(lambda pair: pair[0] < pair[1]) \
        .cartesian(rdd) \
        .coalesce(256) \
        .filter(lambda pair: pair[0][1] < pair[1]) \
        .map(calc_var(t410, vector_map_broadcast)) \
        .filter(lambda pair: pair[1] <= 20) \
        .map(lambda pair: '-'.join(map(str, pair[0]))) \
        .collect()
    t20 = len(triples20)

    print(f">> τ=410: {t410.value}")
    print(f">> τ=20 triples: {triples20}")
    print(f">> τ=20: {t20}")

    print(f">> seconds to calculate: {time.perf_counter() - start:0.2f}")
    print(">> partition counts: 64-128-256")
    rdd.unpersist()
