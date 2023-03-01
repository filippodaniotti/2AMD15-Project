import time
from typing import List

from pyspark import StorageLevel
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, FloatType

from matplotlib import pyplot as plt

import statistics


def question2(df: DataFrame):
    start = time.perf_counter()

    variance_df = calc_variances(df)

    t_values = [20.0, 50.0, 310.0, 360.0, 410.0]
    results = [query(variance_df, t) for t in t_values]

    for t, res in zip(t_values, results):
        print(f"τ={t}: {res}")
    print(f"seconds to calculate: {start - time.perf_counter():0.2f}")

    plot(list(map(str, t_values)), results)


def calc_variances(df: DataFrame) -> DataFrame:
    def calculate_var(row):
        return statistics.variance(row)

    def calculate_agg(row):
        return [x+y for x, y in zip(row[0], row[1])]

    var_udf = F.udf(calculate_var, 'float')
    agg_udf = F.udf(calculate_agg, ArrayType(FloatType()))

    df_with_arr = df.withColumn('ARR', F.array(
        df.columns[1:])).select('_1', 'ARR')
    return df_with_arr \
        .crossJoin(df_with_arr.selectExpr('_1 as _2', 'ARR as ARR2'))\
        .filter('_1 != _2')\
        .withColumn('ARR_AGG_1', agg_udf(F.array('ARR', 'ARR2')))\
        .select('_1', 'ARR_AGG_1')\
        .crossJoin(df_with_arr.selectExpr('_1 as _3', 'ARR as ARR3'))\
        .filter('_1 != _3')\
        .withColumn('AGG', agg_udf(F.array('ARR_AGG_1', 'ARR3')))\
        .select('_1', 'AGG')\
        .withColumn(
            'var',
            var_udf(F.col('AGG'))
        ).persist(StorageLevel.DISK_ONLY)


def query(df: DataFrame, t: float) -> int:
    return df.filter(f'var <= {t}').count()


def plot(t_values: List[str], results: List[int]):
    plt.bar(t_values, results, width=0.4)

    plt.xlabel("τ values")
    plt.ylabel("triples found")
    plt.title("Triples found as τ increases")

    plt.savefig('question2.png')
