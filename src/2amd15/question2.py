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
# 16-32-64
# 64-128-256
# 64-64-64


#_BEGIN_CODE
def question2(df: DataFrame):
    print('>> executing question 2')
    print('>> partitions: 16-32-64')
    start = time.perf_counter()

    t_values = [20.0, 50.0, 310.0, 360.0, 410.0]
    variance_df = calc_variances(df)
    results = [query(variance_df, t) for t in t_values]
    variance_df.unpersist()

    for t, res in zip(t_values, results):
        print(f">> τ={t}: {len(res)}")
        if t in [20.0, 50.0]:
            print(f">> triples: {', '.join(res)}")
    print(f">> seconds to calculate: {time.perf_counter() - start:0.2f}")

    if configuration.ENABLE_EVALUATION:
        plot(list(map(str, t_values)), [len(row) for row in results])


def calc_variances(df: DataFrame) -> DataFrame:
    df_with_arr = df \
        .withColumn('ARR', F.array(df.columns[1:])).select('_1', 'ARR')

    bc = df_with_arr.rdd.context.broadcast({
        key: np.array(value, dtype=np.int16) for (key, value)
        in df_with_arr.rdd.collectAsMap().items()
    })

    df_no_arr = df.select('_1').repartition(16)

    var_udf = F.udf(
        lambda row: np.var(bc.value[row[0]] + bc.value[row[1]] + bc.value[row[2]]).item(), 
        'float'
    )

    return df_no_arr \
        .crossJoin(df_no_arr.selectExpr('_1 as _2')) \
        .coalesce(32)\
        .filter('_1 < _2')\
        .crossJoin(df_no_arr.selectExpr('_1 as _3'))\
        .filter('_2 < _3')\
        .coalesce(64)\
        .withColumn('full_id', F.array('_1', '_2', '_3'))\
        .withColumn(
            'var',
            var_udf(F.col('full_id'))) \
        .select('full_id', 'var')\
        .filter('var <= 410')\
        .persist(StorageLevel.MEMORY_ONLY)


def query(df: DataFrame, t: float) -> List[str]:
    return [
        '-'.join(row.full_id) for row in df.filter(f'var <= {t}')
        .select('full_id')
        .collect()
    ]

