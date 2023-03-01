import time
import statistics
from typing import List

from pyspark import StorageLevel
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, FloatType

can_plot = True
try:
    from matplotlib import pyplot as plt
except ImportError:
    can_plot = False


def question2(df: DataFrame):
    start = time.perf_counter()

    variance_df = calc_variances(df)

    t_values = [20.0, 50.0, 310.0, 360.0, 410.0]
    results = [query(variance_df, t) for t in t_values]

    for t, res in zip(t_values, [len(row) for row in results]):
        print(f"τ={t}: {res}")
    print(f"seconds to calculate: {time.perf_counter() - start:0.2f}")
    print(f"ids for τ-value 20: {results[0]}")

    if can_plot:
        plot(list(map(str, t_values)), [len(row) for row in results])


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
        .select('_1', '_2', 'ARR_AGG_1')\
        .crossJoin(df_with_arr.selectExpr('_1 as _3', 'ARR as ARR3'))\
        .filter('_1 != _3')\
        .withColumn('full_id', F.sort_array(F.array('_1', '_2', '_3')))\
        .drop_duplicates(['full_id'])\
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


def plot(t_values: List[str], results: List[int]):
    ax = plt.axes()
    ax.bar(t_values, results, width=0.4)

    ax.set_xlabel("τ values")
    ax.set_ylabel("triples found")
    ax.set_title("Triples found as τ increases")

    for rect, label in zip(ax.patches, results):
        height = rect.get_height()
        ax.text(
            rect.get_x() + rect.get_width() / 2,
            height + 5, str(label), ha="center", va="bottom"
        )

    plt.savefig('question2.png')
