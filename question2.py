from typing import List
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from matplotlib import pyplot as plt

import statistics


def question2(df: DataFrame):
    t_values = [20.0, 50.0, 310.0, 360.0, 410.0]
    results = [query(df, t) for t in t_values]

    print(zip(t_values, results))

    plot(list(map(str, t_values)), results)


def query(df: DataFrame, t: float) -> int:
    def calculate_var(row):
        return statistics.variance(row)

    var_udf = F.udf(calculate_var, 'float')

    df_with_sum = df.withColumn('SUM', sum(
        [F.col(c) for c in df.columns[1:]]  # type: ignore
    )).select('_1', 'SUM')
    return df_with_sum \
        .crossJoin(df_with_sum.selectExpr('_1 as _2', 'SUM as SUM2'))\
        .filter('_1 != _2')\
        .crossJoin(df_with_sum.selectExpr('_1 as _3', 'SUM as SUM3'))\
        .filter('_1 != _3')\
        .withColumn(
            'var',
            var_udf(F.array('SUM', 'SUM2', 'SUM3'))
        ).filter(f'var <= {t}').count()


def plot(t_values: List[str], results: List[int]):
    plt.bar(t_values, results, width=0.4)

    plt.xlabel("τ values")
    plt.ylabel("triples found")
    plt.title("Triples found as τ increases")

    plt.savefig('question2.png')
