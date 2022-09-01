from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StructField, StructType
from pyspark.sql.window import Window

with SparkSession.builder.appName("AOC_Day_01").getOrCreate() as spark:
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField("col1", IntegerType(), True)])
    df = spark.read.csv("./data/day_01.txt", schema=schema)

    window = Window.partitionBy(func.lit(1)).rowsBetween(-1, 0)
    df.show()

    part_one_df = (
        df.withColumn("list", func.collect_list("col1").over(window))
        .withColumn(
            "result",
            func.col("list")[0] < func.col("list")[1],
        )
        .groupBy("result")
        .count()
    )
    part_one_df.show()

    window_2 = Window.partitionBy(func.lit(1)).rowsBetween(0, 2)
    part_two_df = (
        df.withColumn("list", func.collect_list("col1").over(window_2))
        .withColumn(
            "sum",
            func.aggregate(func.col("list"), func.lit(0), lambda x, acc: x + acc),
        )
        .withColumn("list2", func.collect_list("sum").over(window))
        .withColumn(
            "result",
            func.col("list2")[0] < func.col("list2")[1],
        )
        .groupBy("result")
        .count()
    )
    part_two_df.show()
