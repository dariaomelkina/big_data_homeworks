import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName('SimpleSparkProject').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Videos
    df_videos = spark.read.options(header=True).csv('japan_data/JPvideos.csv').na.drop(subset=["title"])

    # Categories
    df_categories = spark.createDataFrame(pd.read_json('japan_data/JP_category_id.json')["items"])
    df_categories = df_categories.withColumn("channelId", df_categories.snippet['channelId'])
    df_categories = df_categories.withColumn("title", df_categories.snippet['title'])
    df_categories = df_categories.withColumn("assignable", df_categories.snippet['assignable'])
    df_categories = df_categories.drop('snippet')

    # Answers
    print("1) Find Top 10 videos that were amongst the trending videos for the highest number of days "
          "(it doesn't need to be a consecutive period of time). You should also include "
          "information about different metrics for each day the video was trending.")

    # trending_day_schema = StructType([StructField("date", StringType(), True),
    #                                   StructField("views", LongType(), True),
    #                                   StructField("likes", LongType(), True),
    #                                   StructField("dislikes", LongType(), True),
    #                                   ])
    # video_schema = StructType([StructField("id", StringType(), True),
    #                            StructField("title", StringType(), True),
    #                            StructField("description", StringType(), True),
    #                            StructField("latest_views", LongType(), True),
    #                            StructField("latest_likes", LongType(), True),
    #                            StructField("latest_dislikes", LongType(), True),
    #                            StructField("trending_days", ArrayType(trending_day_schema), True),
    #                            ])
    # answer1_schema = StructType([StructField("videos", ArrayType(video_schema), True)])
    # answer1_df = spark.createDataFrame(data=spark.sparkContext.emptyRDD(), schema=answer1_schema)
    # answer1_df.printSchema()

    trending_id = df_videos.groupBy("video_id").count().sort('count', ascending=False).limit(10)\
        .withColumnRenamed("video_id", "id")
    trending_videos = trending_id.join(df_videos, trending_id["id"] == df_videos["video_id"], "inner") \
        .dropDuplicates(["video_id"]).select(["id", "title", "description"])

    trending_videos.show()
    # .withColumnRenamed(
    #     "video_id", "id")
    # df_videos.where(trending_id.video_id.contains(df_videos.video_id)).show(vertical=True)

    # answer1_df.write.json(path='./results/answer1')

    # print("2) Find what was the most popular category for each week (7 days slices). "
    #       "Popularity is decided based on the total number of views for videos of this category. "
    #       "Note, to calculate it you can’t just sum up the number of views.If a particular video appeared only "
    #       "once during the given period, it shouldn’t be counted. Only if it appeared more than once you should "
    #       "count the number of new views. For example, if video A appeared on day 1 with 100 views, then on day 4 "
    #       "with 250 views and again on day 6 with 400 views, you should count it as 400 - 100 = 300. For our purpose, "
    #       "it will mean that this particular video was watched 300 times in the given time period.")
    #
    # print("3) What were the 10 most used tags amongst trending videos for each 30days time period? "
    #       "Note, if during the specified period the same video appears multiple times, "
    #       "you should count tags related to that video only once.")
    #
    # print("4) Show the top 20 channels by the number of views for the whole period. Note, "
    #       "if there are multiple appearances of the same video for some channel, you should take "
    #       "into account only the last appearance (with the highest number of views).")
    #
    # print("5) Show the top 10 channels with videos trending for the highest number of days "
    #       "(it doesn't need to be a consecutive period of time) for the whole period. In order to calculate it, "
    #       "you may use the results from the question №1. The total_trending_days count will be a sum of "
    #       "the numbers of trending days for videos from this channel.")
    #
    # print("6) Show the top 10 videos by the ratio of likes/dislikes for each category for the whole period. "
    #       "You should consider only videos with more than 100K views. If the same video occurs multiple times "
    #       "you should take the record when the ratio was the highest.")
