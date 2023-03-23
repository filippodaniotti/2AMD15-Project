from pyspark import RDD
import numpy as np
import time
import math
def question4(rdd: RDD):
    start = time.perf_counter()
            

    hash_vec = [lambda x: (41651 * x + 97879)%71909, lambda x: (39359* x + 35051)%14879, lambda x: (17881* x + 93911)%56911]

    def create_CM(vector, depth, width):
        table = np.zeros([depth, width])  # Create empty table
        for i in range(0, 10000):
          for d in range(0, depth):
            index = hash_vec[d](i) % width
            table[d, index] += vector[i]
        return table

    def merge_and_variance(key1, key2, key3, depth, broadcast):
        agregate = broadcast.value[key1] + broadcast.value[key2] + broadcast.value[key3]
        return min(np.sum(agregate[i]**2)/10000 -(np.sum(agregate[i])/10000)**2 for i in range(0, depth))
 

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

    print('>> functionality 1:')
    ε_values=[0.001, 0.01]
    for ε in ε_values:
      t400 = calculate_variances(cartesianKeys, rdd, ε, 0.1).filter(lambda pair: pair <= 400)\
             .count()
      print(f">>   τ<400 for ε ={ε}: {t400}")
    print(f">>  seconds to calculate (including the cartesian keys): {time.perf_counter() - start:0.2f}")

    start = time.perf_counter()
    print('>> functionality 2:')
    ε_values=[0.0001, 0.001, 0.002, 0.01]
    for ε in ε_values:
      t200000Rdd = calculate_variances(cartesianKeys, rdd, ε, 0.1).filter(lambda pair: pair >= 200000)\
              .cache()     
      t200000 = t200000Rdd.count()
      t1000000 = t200000Rdd.filter(lambda pair: pair >= 1000000).count()
      t200000Rdd.unpersist()
      print(f">>   τ>200000 for ε ={ε}: {t200000}")
      print(f">>   τ>1000000 for ε ={ε}: {t1000000}")
    
    cartesianKeys.unpersist()
    print(f">>  seconds to calculate (cartesian keys already done in the previous functionality): {time.perf_counter() - start:0.2f}")