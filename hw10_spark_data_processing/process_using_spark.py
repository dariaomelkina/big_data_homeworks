import datetime
import json

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def process_date(date):
    """
    year.day.month =>  datetime(year, month, day)
    """
    date_split = date.split(".")
    return datetime.datetime(int(date_split[0]), int(date_split[2]), int(date_split[1]))


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

    trending_id = df_videos.groupBy("video_id").count().sort('count', ascending=False).limit(10) \
        .withColumnRenamed("video_id", "id")
    trending_videos = trending_id.join(df_videos, trending_id["id"] == df_videos["video_id"], "inner") \
        .dropDuplicates(["video_id"]).select(["id", "title", "description"])
    trending_days = {tr_id[0]: df_videos.where(df_videos["video_id"] == tr_id[0])
        .select("trending_date", "views", "likes", "dislikes")
        .rdd.map(lambda r: {"date": r[0], "views": r[1], "likes": r[2], "dislikes": r[3]}).collect()
                     for tr_id in trending_videos.select("id").collect()}
    latest_days = dict()
    for video_id, data in trending_days.items():
        details = {process_date(day["date"]): {"views": day["views"],
                                               "likes": day["likes"],
                                               "dislikes": day["dislikes"]} for day in data}
        latest_days[video_id] = details[max(details.keys())]

    answer_1 = {"videos": []}
    for video in trending_videos.collect():
        answer_1["videos"].append({"id": video["id"],
                                   "title": video["title"],
                                   "description": video["description"],
                                   "latest_views": latest_days[video["id"]]["views"],
                                   "latest_likes": latest_days[video["id"]]["likes"],
                                   "latest_dislikes": latest_days[video["id"]]["dislikes"],
                                   "trending_days": trending_days[video["id"]]
                                   })

    json_object = json.dumps(answer_1)
    with open("results/answer1.json", "w") as outfile:
        outfile.write(json_object)

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
