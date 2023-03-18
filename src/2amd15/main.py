from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
import numpy as np
#_END_IMPORTS

import configuration
from question2 import question2
from question3 import question3

#_BEGIN_CODE


def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf()\
        .setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")\
            .set("spark.driver.memory", "6g")\
            .set("spark.executor.memory", "8g")
    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info.
        # For larger data sets, to not set the log level to anything below WARN,
        # Spark will print too much information.
        spark_context.setLogLevel("ERROR")

    return spark_context


def q1a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    spark_session = SparkSession(spark_context)

    if on_server:
        rdd = spark_context.textFile("hdfs:/vectors.csv", 64)\
            .map(lambda line: tuple(
                [line.split(',', 1)[0]]
                + [float(x) for x in line.split(',', 1)[1].split(';')]
            ))
        df = rdd.toDF([("_" + str(k + 1)) for k in range(len(rdd.take(1)[0]))])
        return df

    vectors_file_path = "vectors.csv"

    # TODO: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}.

    with open(vectors_file_path) as f:
        lines = [line.strip().split(',') for line in f.readlines()]
        return spark_session.createDataFrame(
            [tuple([line[0]] + [float(x) for x in line[1].split(';')])
             for line in lines]
        )


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    if on_server:
        return spark_context.textFile("hdfs:/vectors.csv", 80).map(
            lambda line: tuple([
                line.split(',', 1)[0],
                np.array([np.int16(int(x)) for x in line.split(',', 1)[1].split(';')])
            ])
        )

    vectors_file_path = "vectors.csv"

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    with open(vectors_file_path) as f:
        return spark_context.parallelize(
            [tuple([
                line.split(',', 1)[0],
                np.array([float(x) for x in line.split(',', 1)[1].split(';')])
            ]) for line in f.readlines()]
        )


def q2(spark_context: SparkContext, data_frame: DataFrame):
    question2(data_frame)


def q3(spark_context: SparkContext, rdd: RDD):
    question3(rdd)


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q4 here
    return


if __name__ == '__main__':

    on_server = configuration.ON_SERVER

    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server)

    rdd = q1b(spark_context, on_server)

    # q2(spark_context, data_frame)

    q3(spark_context, rdd)

    q4(spark_context, rdd)

    spark_context.stop()
