import time
from statistics import variance
from typing import List

from pyspark import StorageLevel
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, FloatType

from src.evaluation import plot, is_evaluation_enabled

def question2(df: DataFrame):
    start = time.perf_counter()

    variance_df = calc_variances(df)

    t_values = [20.0, 50.0, 310.0, 360.0, 410.0]
    results = [query(variance_df, t) for t in t_values]
    variance_df.unpersist()

    for t, res in zip(t_values, [len(row) for row in results]):
        print(f"τ={t}: {res}")
    print(f"seconds to calculate: {time.perf_counter() - start:0.2f}")

    if is_evaluation_enabled():
        plot(list(map(str, t_values)), [len(row) for row in results])


def calc_variances(df: DataFrame) -> DataFrame:
    var_udf = F.udf(lambda row: variance(row), 'float')
    agg_udf = F.udf(
        lambda row: [x+y for x, y in zip(row[0], row[1])], 
        ArrayType(FloatType())
    )

    df_with_arr = df.withColumn('ARR', F.array(df.columns[1:])).select('_1', 'ARR')
    
    return df_with_arr \
        .crossJoin(df_with_arr.selectExpr('_1 as _2', 'ARR as ARR2'))\
        .filter('_1 < _2')\
        .withColumn('ARR_AGG_1', agg_udf(F.array('ARR', 'ARR2')))\
        .crossJoin(df_with_arr.selectExpr('_1 as _3', 'ARR as ARR3'))\
        .filter('_2 < _3')\
        .withColumn('full_id', F.array('_1', '_2', '_3'))\
        .withColumn('AGG', agg_udf(F.array('ARR_AGG_1', 'ARR3')))\
        .withColumn(
            'var',
            var_udf(F.col('AGG'))
        ).select('full_id', 'var')\
        .persist(StorageLevel.MEMORY_ONLY)

def query(df: DataFrame, t: float) -> List[str]:
    return [
        '-'.join(row.full_id) for row in df.filter(f'var <= {t}')
        .select('full_id')
        .collect()
    ]