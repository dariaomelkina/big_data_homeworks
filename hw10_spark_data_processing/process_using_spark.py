from pyspark.sql import SparkSession
from pyspark.sql.types import *

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
    category_schema = StructType([
        StructField('kind', StringType()),
        StructField('etag', StringType()),
        StructField('items',
                    ArrayType(StructType([
                        StructField('kind', StringType()),
                        StructField('etag', StringType()),
                        StructField('id', StringType()),

                        StructField('snippet', StructType([StructField('channelId', StringType()),
                                                           StructField('title', StringType()),
                                                           StructField('assignable', BooleanType())])),
                    ])))
    ])
    df_categories = spark.read.json('japan_data/JP_category_id.json', schema=category_schema)

    df_categories.select("items").show()

    # sqlContext.read.json('japan_data/JP_category_id.json', schema=category_schema).registerTempTable('df')
    # sqlContext.sql("select explode(col) from (select explode(col.items) from df)").show()
