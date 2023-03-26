from pyspark import RDD, SparkContext
import numpy as np
import time
import math

from typing import Any
#_END_IMPORTS


#_BEGIN_CODE
def question4(spark_context: SparkContext, rdd: RDD) -> None:
    print('>> executing question 4')
    print('>> vector count ', rdd.count())
    print('>> vector length ', len(rdd.take(1)[0][1]))
    start = time.perf_counter()
            

    hash_vec = [
        lambda x: (41651 * x + 415721) % 530531, 
        lambda x: (39359 * x + 653593) % 761023, 
        lambda x: (17881 * x + 277003) % 806783
    ]

    def create_CM(vector, depth, width) -> np.ndarray:
        table = np.zeros([depth, width])  # Create empty table
        for vec_idx, value in enumerate(vector):
            for depth_idx, hash_fn in enumerate(hash_vec):
                width_idx = hash_fn(vec_idx) % width
                table[depth_idx, width_idx] += value
        return table
        
    def merge_and_variance(key1, key2, key3, broadcast) -> float:
        aggregate = broadcast.value[key1] + broadcast.value[key2] + broadcast.value[key3]
        inner = np.sum(aggregate * aggregate, axis=1)
        return np.min(inner)/10000 - (np.sum(aggregate[0])/10000)**2
 

    def calculate_variances(
            cartesian_keys: RDD, 
            rdd_keys: RDD, 
            vector_map_broadcast: Any, 
            epsilon: float, 
            delta: float) -> RDD:
        depth = math.ceil(math.log(1 / delta))
        width = math.ceil(math.e / epsilon)

        def get_sketches(broadcast):
            return lambda key: (key, create_CM(broadcast.value[key], depth, width))

        sketch_map = rdd_keys \
            .map(get_sketches(vector_map_broadcast)) \
            .collectAsMap() 
                          
        sketch_map_broadcast = rdd.context.broadcast(sketch_map)

        return cartesian_keys \
            .map(lambda keys: merge_and_variance(keys[0][0], keys[0][1], keys[1], sketch_map_broadcast))


    vector_map = rdd.collectAsMap()
    vector_map_broadcast = rdd.context.broadcast(vector_map)

    rdd_keys = rdd.keys().cache()

    cartesian_keys = rdd_keys \
        .cartesian(rdd_keys) \
        .coalesce(128) \
        .filter(lambda pair: pair[0] < pair[1]) \
        .cartesian(rdd_keys) \
        .coalesce(256) \
        .filter(lambda pair: pair[0][1] < pair[1]) \
        .cache()
        

    epsilon_values = [0.0001, 0.001, 0.002, 0.01]
    for epsilon in epsilon_values:
        var = calculate_variances(cartesian_keys, rdd_keys, vector_map_broadcast, epsilon, 0.1).cache()
        if epsilon in [0.001, 0.01]:
            t400 = var.filter(lambda pair: pair <= 400).count()
            print('>> functionality 1:')
            print(f">>\t τ < 400 for ε = {epsilon}: {t400}")

        t200000 = var.filter(lambda pair: pair >= 200000).count()
        t1000000 = var.filter(lambda pair: pair >= 1000000).count()
        print('>> functionality 2:')
        print(f">>\tτ > 200000 for ε = {epsilon}: {t200000}")
        print(f">>\tτ > 1000000 for ε = {epsilon}: {t1000000}")

        var.unpersist()

    rdd_keys.unpersist()
    cartesian_keys.unpersist()
    print(f">> seconds to calculate: {time.perf_counter() - start:0.2f}")
