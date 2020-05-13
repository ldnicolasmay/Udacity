from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    Example program to show how to submit applications
    """

    spark = SparkSession\
        .builder\
        .appName("LowerSongTitles")\
        .getOrCreate()

    log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the starts"
    ]

    distributed_song_log = spark.sparkContext.parallelize(log_of_songs)

    print(distributed_song_log.map(lambda x: x.lower()).collect())

    spark.stop()

