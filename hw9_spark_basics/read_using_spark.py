from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName('SimpleSparkProject').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.csv('PS_20174392719_1491204439457_log.csv')
    print("#" * 100 + f"\nNumber of rows is: {df.count()}\n" + "#" * 100)
