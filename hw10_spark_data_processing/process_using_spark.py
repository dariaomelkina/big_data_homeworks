import calendar
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
    print("Answering question 1...")
    trending_id = df_videos.groupBy("video_id").count().sort('count', ascending=False).limit(10) \
        .withColumnRenamed("video_id", "id")
    trending_videos = trending_id.join(df_videos, trending_id["id"] == df_videos["video_id"], "inner") \
        .dropDuplicates(["video_id"]).select(["id", "title", "description"])
    trending_days = {tr_id[0]: df_videos.where(df_videos["video_id"] == tr_id[0])
        .select("trending_date", "views", "likes", "dislikes")
        .rdd.map(lambda r: {"date": r[0], "views": r[1], "likes": r[2], "dislikes": r[3]}).collect()
                     for tr_id in trending_videos.select("id").collect()}
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
    # print("Answering question 2...")
    # weeks_category_data = dict()
    # for row in df_videos.collect():
    #     video_id = row["video_id"]
    #     category_id = row["category_id"]
    #     # Using week start as a week key here
    #     video_date = process_date(row["trending_date"])
    #     if not video_date:
    #         continue
    #     week_key = (video_date - datetime.timedelta(days=video_date.weekday())).strftime('%y.%d.%m')
    #
    #     if week_key not in weeks_category_data:
    #         weeks_category_data[week_key] = dict()
    #
    #     if category_id not in weeks_category_data[week_key]:
    #         weeks_category_data[week_key][category_id] = dict()
    #
    #     if video_id not in weeks_category_data[week_key][category_id]:
    #         weeks_category_data[week_key][category_id][video_id] = []
    #
    #     if row["views"]:
    #         weeks_category_data[week_key][category_id][video_id].append(int(row["views"]))
    #
    # weeks = []
    # for date, categories in weeks_category_data.items():
    #     start_date = date
    #     end_date = (process_date(start_date) + datetime.timedelta(days=6)).strftime('%y.%d.%m')
    #
    #     best_category_id = None
    #     total_views = 0
    #     video_ids = []
    #
    #     for category, videos in categories.items():
    #         category_views = 0
    #         for video, video_views in videos.items():
    #             if len(video_views) < 2:
    #                 continue
    #             category_views += sum(video_views[1:]) - video_views[0]
    #
    #         if category_views > total_views:
    #             best_category_id = category
    #             total_views = category_views
    #             video_ids = list(videos.keys())
    #
    #     number_of_videos = len(video_ids)
    #     category_name = df_categories.where(df_categories["id"] == best_category_id).select("title").collect()[0][0]
    #
    #     weeks.append({
    #         "start_date": start_date,
    #         "end_date": end_date,
    #         "category_id": best_category_id,
    #         "category_name": category_name,
    #         "number_of_videos": number_of_videos,
    #         "total_views": total_views,
    #         "video_ids": video_ids
    #     })
    #
    # answer_2 = {"weeks": weeks}
    #
    # json_object = json.dumps(answer_2)
    # with open("results/answer2.json", "w") as outfile:
    #     outfile.write(json_object)

    ################################################################################################################
    # print("Answering question 3...")
    # months_data = dict()
    # for row in df_videos.collect():
    #     # Using year.month as a key here
    #     video_date = process_date(row["trending_date"])
    #     if not video_date:
    #         continue
    #     month_key = video_date.strftime('%y.%m')
    #
    #     if month_key not in months_data:
    #         months_data[month_key] = dict()
    #
    #     tags = row["tags"]
    #     for tag in tags:
    #         if tag not in months_data[month_key]:
    #             months_data[month_key][tag] = set()
    #         months_data[month_key][tag].add(row["video_id"])
    #
    # answer_3 = {"months": []}
    # for month, tags in months_data.items():
    #     tags_items = list(tags.items())
    #     popular_tags = dict(sorted(tags_items, key=lambda x: len(x[1]), reverse=True)[:10])
    #
    #     start_date = datetime.datetime(int(month.split(".")[0]), int(month.split(".")[1]), 1)
    #     end_date = start_date.replace(day=calendar.monthrange(start_date.year, start_date.month)[1])
    #
    #     answer_3["months"].append({
    #         "start_date": start_date.strftime('%y.%d.%m'),
    #         "end_date": end_date.strftime('%y.%d.%m'),
    #         "tags": [{"tag": tag,
    #                   "number_of_videos": len(videos),
    #                   "video_ids": list(videos)}
    #                  for tag, videos in popular_tags.items()]
    #     })
    #
    # json_object = json.dumps(answer_3)
    # with open("results/answer3.json", "w") as outfile:
    #     outfile.write(json_object)

    ################################################################################################################
    # print("Answering question 4...")
    # dates = [process_date(date[0]) for date in df_videos.select("trending_date").collect() if process_date(date[0])]
    # start_date = min(dates).strftime('%y.%d.%m')
    # end_date = max(dates).strftime('%y.%d.%m')
    #
    # channels_views = dict()
    # for row in df_videos.collect():
    #     channel = row['channel_title']
    #     if channel not in channels_views:
    #         channels_views[channel] = 0
    #     try:
    #         channels_views[channel] += int(row['views'])
    #     except:
    #         continue
    #
    # channel_items = list(channels_views.items())
    # top_channels = dict(sorted(channel_items, key=lambda x: x[1], reverse=True)[:20])
    #
    # answer_4 = {"channels": []}
    # for channel, total_views in top_channels.items():
    #     videos_data = df_videos.where(df_videos['channel_title'] == channel).collect()
    #
    #     answer_4["channels"].append({
    #         "channel_name": channel,
    #         "start_date": start_date,
    #         "end_date": end_date,
    #         "total_views": total_views,
    #         "videos_views": [{"video_id": i["video_id"],
    #                           "views": i["views"]}
    #                          for i in videos_data]
    #     })
    #
    # json_object = json.dumps(answer_4)
    # with open("results/answer4.json", "w") as outfile:
    #     outfile.write(json_object)

    ################################################################################################################
    print("Answering question 5...")
    trending_videos = trending_id.join(df_videos, trending_id["id"] == df_videos["video_id"], "inner")
    video_ids = []
    answer_5 = {"channels": []}
    for video in trending_videos.collect():
        if video["id"] not in video_ids:
            answer_5["channels"].append({
                "channel_name": video["channel_title"],
                "total_trending_days": len(trending_days[video["id"]]),
                "videos_days": [{
                    "video_id": video["id"],
                    "video_title": video["title"],
                    "trending_days": len(trending_days[video["id"]])
                }]
            })
            video_ids.append(video["id"])

    json_object = json.dumps(answer_5)
    with open("results/answer5.json", "w") as outfile:
        outfile.write(json_object)


    ################################################################################################################
    # print("Answering question 6...")
    # print("6) Show the top 10 videos by the ratio of likes/dislikes for each category for the whole period. "
    #       "You should consider only videos with more than 100K views. If the same video occurs multiple times "
    #       "you should take the record when the ratio was the highest.")

    # print("Finished creating all answers.")

    # dates = [process_date(date[0]) for date in df_videos.select("trending_date").collect() if process_date(date[0])]
    # curr_date = min(dates)
    # thirty_day_period_starts = []
    # while curr_date <= max(dates):
    #     thirty_day_period_starts.append(curr_date)
    #     curr_date = curr_date + datetime.timedelta(days=30)
    #
    # print(thirty_day_period_starts)
