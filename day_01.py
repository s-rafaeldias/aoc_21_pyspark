from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import types
from pyspark.sql.types import IntegerType, StructField, StructType
from pyspark.sql.window import Window

with SparkSession.builder.appName("AOC_Day_01").getOrCreate() as spark:
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField("col1", IntegerType(), True)])
    df = spark.read.csv("./data/day_01.txt", schema=schema)

    window = Window.partitionBy(func.lit(1)).rowsBetween(-1, 0)
    df.show()

    part_one_df = (
        df.withColumn(
            "part_one_list", func.collect_list("col1").over(window)
        ).withColumn(
            "part_one_result",
            func.col("part_one_list")[0] > func.col("part_one_list")[1],
        )
        .groupBy("part_one_result")
        .count()
    )
    part_one_df.show()
