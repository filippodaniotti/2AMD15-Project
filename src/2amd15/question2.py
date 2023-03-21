import time
import numpy as np
from statistics import pvariance
from typing import List

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, FloatType
import pyspark.sql.functions as F
#_END_IMPORTS

import configuration
from evaluation import plot


#_BEGIN_CODE
def question2(df: DataFrame):
    print('>> executing question 2')
    start = time.perf_counter()

    t_values = [20.0, 50.0, 310.0, 360.0, 410.0]
    variance_df = calc_variances(df)
    variance410 = variance_df.filter(f'var <= {t_values[-1]}').persist(StorageLevel.MEMORY_ONLY)
    t410 = variance410.count()
    results = [query(variance410, t) for t in t_values[:-1]]
    variance410.unpersist()

    for t, res in zip(t_values, results):
        print(f">> τ={t}: {len(res)}")
        if t in [20.0, 50.0]:
            print(f">> triples: {', '.join(res)}")
    print(f">> τ={t_values[-1]}: {t410}") 
    print(f">> seconds to calculate: {time.perf_counter() - start:0.2f}")

    if configuration.ENABLE_EVALUATION:
        plot(list(map(str, t_values)), [len(row) for row in results])


def calc_variances(df: DataFrame) -> DataFrame:
    # def a(row):
    #     a = np.var(row)
    #     b = pvariance(row)
    #     print(row)
    #     print(type(a))
    #     print(type(b))
    #     print()
        
    #     return float(a)
    # var_udf = F.udf(a)
    var_udf = F.udf(lambda row: float(np.var(row)), 'float')
    agg_udf = F.udf(
        lambda row: [x+y for x, y in zip(row[0], row[1])],
        ArrayType(FloatType())
    )
    

    df_with_arr = df.withColumn('ARR', F.array(
        df.columns[1:])).select('_1', 'ARR')
    
    # ret = df_with_arr \
    #     .crossJoin(df_with_arr.selectExpr('_1 as _2', 'ARR as ARR2'))\
    #     .repartition(25)\
    #     .filter('_1 < _2')\
    #     .withColumn('ARR_AGG_1', agg_udf(F.array('ARR', 'ARR2')))\
    #     .crossJoin(df_with_arr.selectExpr('_1 as _3', 'ARR as ARR3'))\
    #     .filter('_2 < _3')\
    #     .withColumn('full_id', F.array('_1', '_2', '_3'))\
    #     .withColumn('AGG', agg_udf(F.array('ARR_AGG_1', 'ARR3')))\
    #     .withColumn(
    #         'var',
    #         var_udf(F.col('AGG'))
    #     ).select('full_id', 'var')
    # ret = df_with_arr \
    #     .withColumn(
    #         'var',
    #         var_udf(F.col('ARR'))
    #     ).select('_1', 'var')
    
    # ret.show(5)

    return df_with_arr \
        .crossJoin(df_with_arr.selectExpr('_1 as _2', 'ARR as ARR2'))\
        .repartition(25)\
        .filter('_1 < _2')\
        .withColumn('ARR_AGG_1', agg_udf(F.array('ARR', 'ARR2')))\
        .crossJoin(df_with_arr.selectExpr('_1 as _3', 'ARR as ARR3'))\
        .filter('_2 < _3')\
        .withColumn('full_id', F.array('_1', '_2', '_3'))\
        .withColumn('AGG', agg_udf(F.array('ARR_AGG_1', 'ARR3')))\
        .withColumn(
            'var',
            var_udf(F.col('AGG'))
        ).select('full_id', 'var')
    return ret

def query(df: DataFrame, t: float) -> List[str]:
    return [
        '-'.join(row.full_id) for row in df.filter(f'var <= {t}')
        .select('full_id')
        .collect()
    ]
