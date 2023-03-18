import time
from pyspark import RDD
from statistics import pvariance, mean
#_END_IMPORTS


#_BEGIN_CODE
def question3(rdd: RDD):
    print('>> spark.executor.instances = ', rdd.context.getConf().get("spark.executor.instances"))
    print('>> spark.executor.cores = ', rdd.context.getConf().get("spark.executor.cores"))
    print('>> spark.executor.memory = ', rdd.context.getConf().get("spark.executor.memory"))
    print('>> spark.driver.memory = ', rdd.context.getConf().get("spark.driver.memory"))
    print('>> all hosts = ', rdd.context._jsc.sc().getExecutorMemoryStatus().keys())
    print('>> hosts len = ', rdd.context._jsc.sc().getExecutorMemoryStatus().keys().size())
    print('>> executing question 3')
    print('>> vector count ', rdd.count())
    print('>> vector length ', len(rdd.take(1)[0][1]))
    start = time.perf_counter()

    vectorMap = rdd.map(lambda pair: (
        pair[0],
        (pair[1], mean(pair[1]))
    )).collectAsMap()

    rdd = rdd.keys()
    print(">> partitions at the start: ", rdd.getNumPartitions())

    variance410 = rdd\
        .cartesian(rdd)\
        .coalesce(80)\
        .filter(lambda pair: pair[0] < pair[1])\
        .cartesian(rdd)\
        .coalesce(160)\
        .filter(lambda pair: pair[0][1] < pair[1])
    print(">> partitions after cartesians: ", variance410.getNumPartitions())

    vectorMapBroadcast = variance410.context.broadcast(vectorMap)

    t410 = variance410.context.accumulator(0)

    def calc_var(acc, broadcast):
        def inner(keys):
            value0 = broadcast.value[keys[0][0]]
            value1 = broadcast.value[keys[0][1]]
            value2 = broadcast.value[keys[1]]

            var = pvariance(
                [x+y+z for x, y, z in zip(value0[0], value1[0], value2[0])],
                value0[1] + value1[1] + value2[1]
            )
            if var <= 410:
                acc.add(1)

            return (
                (keys[0][0], keys[0][1], keys[1]),
                var
            )
        return inner

    variance410 = variance410\
        .map(calc_var(t410, vectorMapBroadcast))

    print(">> partitions at the end: ", variance410.getNumPartitions())

    triples20 = variance410.filter(lambda pair: pair[1] <= 20)\
        .map(lambda pair: '-'.join(map(str, pair[0])))\
        .collect()
    t20 = len(triples20)

    print(f">> τ=410: {t410.value}")
    print(f">> τ=20 triples: {triples20}")
    print(f">> τ=20: {t20}")

    print(f">> seconds to calculate: {time.perf_counter() - start:0.2f}")
