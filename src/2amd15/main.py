from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
#_END_IMPORTS

import configuration
from question2 import question2
from question3 import question3

#_BEGIN_CODE
def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf()\
        .setAppName("2AMD15")\
        .set("spark.executor.memory", "2g")\
        .set("spark.driver.memory", "3g")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info.
        # For larger data sets, to not set the log level to anything below WARN,
        # Spark will print too much information.
        spark_context.setLogLevel("ERROR")

    return spark_context


def q1a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    spark_session = SparkSession(spark_context)

    # TODO: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}.

    with open(vectors_file_path) as f:
        lines = [line.strip().split(',') for line in f.readlines()]
        return spark_session.createDataFrame(
            [tuple([line[0]] + [float(x) for x in line[1].split(';')])
             for line in lines]
        )


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    with open(vectors_file_path) as f:
        lines = [line.strip().split(',') for line in f.readlines()]
        return spark_context.parallelize(
            [[line[0]] + [float(x) for x in line[1].split(';')]
             for line in lines]
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

    q2(spark_context, data_frame)

    q3(spark_context, rdd)

    q4(spark_context, rdd)

    spark_context.stop()
