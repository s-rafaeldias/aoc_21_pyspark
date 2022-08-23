import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import types
from pyspark.sql.types import IntegerType, StructField, StructType
from pyspark.sql.window import Window


@func.pandas_udf(returnType=types.BooleanType())
def sensor(x: pd.Series) -> bool:
    l = list(x)
    return l[0] < l[-1]


with SparkSession.builder.appName("AOC_Day_01").getOrCreate() as spark:
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField("col1", IntegerType(), True)])
    df = spark.read.csv("./data/day_01.txt", schema=schema)

    window = Window.partitionBy(func.lit(1)).rowsBetween(-1, 0)
    df.show()

    part_one_df = (
        df.withColumn("part_one", sensor("col1").over(window))
        .groupBy("part_one")
        .count()
    )
    part_one_df.show()
