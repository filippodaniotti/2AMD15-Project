from pyspark import RDD
import numpy as np
import time
import math
#_END_IMPORTS


#_BEGIN_CODE
def question4(rdd: RDD):
    start = time.perf_counter()
            

    hash_vec = [lambda x: (41651 * x + 415721)%530531, lambda x: (39359* x + 653593)%761023, lambda x: (17881* x + 277003)%806783]

    def create_CM(vector, depth, width):
        table = np.zeros([depth, width])  # Create empty table
        for i in range(0, 10000):
          for d in range(0, depth):
            index = hash_vec[d](i) % width
            table[d, index] += vector[i]
        return table

    def merge_and_variance(key1, key2, key3, depth, broadcast):
        agregate = broadcast.value[key1] + broadcast.value[key2] + broadcast.value[key3]
        inner = np.sum(agregate * agregate, axis=1)
        return np.min(inner)/10000 - (np.sum(agregate[0])/10000)**2
        # Original solution:
        # return min(np.sum(agregate[i]**2)/10000 -(np.sum(agregate[i])/10000)**2 for i in range(0, depth))
 

    def calculate_variances(cartesianKeys : RDD, rdd : RDD,ε,δ):
        depth = math.ceil(math.log(1/δ))
        width = math.ceil(math.e/ε)

        sketchMap =rdd.map(lambda pair: (pair[0], create_CM(pair[1], depth, width)))\
                      .collectAsMap() 
                                          
        sketchMapBroadcast = rdd.context.broadcast(sketchMap)

        return cartesianKeys.map(lambda keys: merge_and_variance(keys[0][0], keys[0][1], keys[1], depth, sketchMapBroadcast))


    rddKeys = rdd.keys().cache()

    cartesianKeys = rddKeys\
        .cartesian(rddKeys)\
        .coalesce(128)\
        .filter(lambda pair: pair[0] < pair[1])\
        .cartesian(rddKeys)\
        .coalesce(256)\
        .filter(lambda pair: pair[0][1] < pair[1])\
        .cache()

    rddKeys.unpersist()

    ε_values=[0.0001, 0.001, 0.002, 0.01]
    for ε in ε_values:
      var = calculate_variances(cartesianKeys, rdd, ε, 0.1).cache()
      if ε in [0.001, 0.01]:
        t400 = var.filter(lambda pair: pair <= 400).count()
        print('>> functionality 1:')
        print(f">>   τ<400 for ε ={ε}: {t400}")

      t200000 = var.filter(lambda pair: pair >= 200000).count()
      t1000000 = var.filter(lambda pair: pair >= 1000000).count()
      print('>> functionality 2:')
      print(f">>   τ>200000 for ε ={ε}: {t200000}")
      print(f">>   τ>1000000 for ε ={ε}: {t1000000}")

      var.unpersist()

    cartesianKeys.unpersist()
    print(f">>  seconds to calculate: {time.perf_counter() - start:0.2f}")
