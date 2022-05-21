import datetime
import json

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def process_date(given_date):
    """
    year.day.month =>  datetime(year, month, day)
    """
    date_split = given_date.split(".")
    try:
        return datetime.datetime(int(date_split[0]), int(date_split[2]), int(date_split[1]))
    except:
        return None


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
    ################################################################################################################
    # print("Answering question 1...")
    # trending_id = df_videos.groupBy("video_id").count().sort('count', ascending=False).limit(10) \
    #     .withColumnRenamed("video_id", "id")
    # trending_videos = trending_id.join(df_videos, trending_id["id"] == df_videos["video_id"], "inner") \
    #     .dropDuplicates(["video_id"]).select(["id", "title", "description"])
    # trending_days = {tr_id[0]: df_videos.where(df_videos["video_id"] == tr_id[0])
    #     .select("trending_date", "views", "likes", "dislikes")
    #     .rdd.map(lambda r: {"date": r[0], "views": r[1], "likes": r[2], "dislikes": r[3]}).collect()
    #                  for tr_id in trending_videos.select("id").collect()}
    # latest_days = dict()
    # for video_id, data in trending_days.items():
    #     details = {process_date(day["date"]): {"views": day["views"],
    #                                            "likes": day["likes"],
    #                                            "dislikes": day["dislikes"]} for day in data}
    #     latest_days[video_id] = details[max(details.keys())]
    #
    # answer_1 = {"videos": []}
    # for video in trending_videos.collect():
    #     answer_1["videos"].append({"id": video["id"],
    #                                "title": video["title"],
    #                                "description": video["description"],
    #                                "latest_views": latest_days[video["id"]]["views"],
    #                                "latest_likes": latest_days[video["id"]]["likes"],
    #                                "latest_dislikes": latest_days[video["id"]]["dislikes"],
    #                                "trending_days": trending_days[video["id"]]
    #                                })
    #
    # json_object = json.dumps(answer_1)
    # with open("results/answer1.json", "w") as outfile:
    #     outfile.write(json_object)

    ################################################################################################################
    print("Answering question 2...")
    weeks_category_data = dict()
    for row in df_videos.collect():
        video_id = row["video_id"]
        category_id = row["category_id"]
        # Using week start as a week key here
        video_date = process_date(row["trending_date"])
        if not video_date:
            continue
        week_key = (video_date - datetime.timedelta(days=video_date.weekday())).strftime('%y.%d.%m')

        if week_key not in weeks_category_data:
            weeks_category_data[week_key] = dict()

        if category_id not in weeks_category_data[week_key]:
            weeks_category_data[week_key][category_id] = dict()

        if video_id not in weeks_category_data[week_key][category_id]:
            weeks_category_data[week_key][category_id][video_id] = []

        if row["views"]:
            weeks_category_data[week_key][category_id][video_id].append(int(row["views"]))

    weeks = []
    for date, categories in weeks_category_data.items():
        start_date = date
        end_date = (process_date(start_date) + datetime.timedelta(days=6)).strftime('%y.%d.%m')

        best_category_id = None
        total_views = 0
        video_ids = []

        for category, videos in categories.items():
            category_views = 0
            for video, video_views in videos.items():
                if len(video_views) < 2:
                    continue
                category_views += sum(video_views[1:]) - video_views[0]

            if category_views > total_views:
                best_category_id = category
                total_views = category_views
                video_ids = list(videos.keys())

        number_of_videos = len(video_ids)
        category_name = df_categories.where(df_categories["id"] == best_category_id).select("title").collect()[0][0]

        weeks.append({
            "start_date": start_date,
            "end_date": end_date,
            "category_id": best_category_id,
            "category_name": category_name,
            "number_of_videos": number_of_videos,
            "total_views": total_views,
            "video_ids": video_ids
        })

    answer_2 = {"weeks": weeks}

    json_object = json.dumps(answer_2)
    with open("results/answer2.json", "w") as outfile:
        outfile.write(json_object)

    ################################################################################################################
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
