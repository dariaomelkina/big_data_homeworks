from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd

if __name__ == "__main__":
    spark = SparkSession.builder.appName('SimpleSparkProject').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Videos
    df_videos = spark.read.options(header=True).csv('japan_data/JPvideos.csv')
    # print(df_videos)
    # print("#" * 100 + f"\nNumber of rows is: {df_videos.count()}\n" + "#" * 100)
    # df_videos.printSchema()
    # df_videos.show(vertical = True)

    # Categories
    df_categories = spark.createDataFrame(pd.read_json('japan_data/JP_category_id.json')["items"])
    df_categories = df_categories.withColumn("channelId", df_categories.snippet['channelId'])
    df_categories = df_categories.withColumn("title", df_categories.snippet['title'])
    df_categories = df_categories.withColumn("assignable", df_categories.snippet['assignable'])
    df_categories = df_categories.drop('snippet')
    # print(df_categories)
    # df_categories.show()
