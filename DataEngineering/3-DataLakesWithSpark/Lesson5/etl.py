import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofweek, dayofmonth, hour, weekofyear
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, LongType, TimestampType


# parse AWS credentials from config file
config = configparser.ConfigParser()
config.read('dl.cfg')

# set OS environment variables for use by Spark
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a SparkSession object and returns it

    :return: SparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes song data
     (1) reads all the single-record json files from input_data S3 bucket into song_data dataframe
     (2) selects appropriate song_data columns into songs_table dataframe
     (3) writes songs_table dataframe in parquet format to output_data S3 bucket
     (4) selects appropriate song_data columns into artists_table dataframe
     (5) writes artists_table dataframe in parquet format to output_data S3 bucket

    :param spark: SparkSession object
    :param input_data: S3 bucket URI string to read json files from
    :param output_data: S3 bucket URI string to write processed dataframes to
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # define song_data schema
    song_data_schema = StructType([
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', DoubleType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_longitude', DoubleType(), True),
        StructField('artist_name', StringType(), True),
        StructField('duration', DoubleType(), True),
        StructField('num_songs', LongType(), True),
        StructField('song_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('year', LongType(), True),
    ])

    # read song data file
    print("\n---- song df ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    song_df = spark.read.json(song_data, schema=song_data_schema)

    # extract columns to create songs table
    print("\n---- songs_table ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    songs_table = song_df \
        .select([
            'song_id',
            'title',
            'artist_id',
            'artist_name',
            'year',
            'duration'
        ]) \
        .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    print("\n---- write songs_table ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    songs_table.write.parquet(output_data + "data_lake/songs_table_parquet",
                              mode="overwrite", partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    print("\n---- artists_table ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    artists_table = song_df \
        .select([
            'artist_id',
            'artist_name',
            'artist_location',
            'artist_latitude',
            'artist_longitude'
        ]) \
        .dropDuplicates()

    # write artists table to parquet files
    print("\n---- write artists_table ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    artists_table.write.parquet(output_data + "data_lake/artists_table_parquet",
                                mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Processes song data
     (1) reads all the json files from input_data S3 bucket into log_data dataframe
     (2) selects appropriate log_data columns into users_table dataframe
     (3) writes users_table dataframe in parquet format to output_data S3 bucket
     (4) mutates log_data `ts` unix epoch column to TimestampType column `start_time`
     (5) selects appropriate log_data column into time_table dataframe
     (6) writes time_table dataframe in parquet format to output_data S3 bucket

    :param spark: SparkSession object
    :param input_data: S3 bucket URI string to read json files from
    :param output_data: S3 bucket URI string to write processed dataframes to
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # define log_data schema
    log_data_schema = StructType([
        StructField('artist', StringType(), True),
        StructField('auth', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('itemInSession', LongType(), True),
        StructField('lastName', StringType(), True),
        StructField('length', DoubleType(), True),
        StructField('level', StringType(), True),
        StructField('location', StringType(), True),
        StructField('method', StringType(), True),
        StructField('page', StringType(), True),
        StructField('registration', DoubleType(), True),
        StructField('sessionId', LongType(), True),
        StructField('song', StringType(), True),
        StructField('status', LongType(), True),
        StructField('ts', LongType(), True),
        StructField('userAgent', StringType(), True),
        StructField('userId', StringType(), True)
    ])

    # read log data file
    print("\n---- log df ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    log_df = spark.read.json(log_data, schema=log_data_schema)
    log_df = log_df \
        .withColumnRenamed('firstName', 'first_name') \
        .withColumnRenamed('lastName', 'last_name') \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('itemInSession', 'item_in_session') \
        .withColumnRenamed('sessionId', 'session_id') \
        .withColumnRenamed('userAgent', 'user_agent')

    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == "NextSong")

    # extract columns for users table
    print("\n---- users_table ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    users_table = log_df \
        .select([
            'user_id',
            'first_name',
            'last_name',
            'gender',
            'level'
        ]) \
        .dropDuplicates()

    # write users table to parquet files
    print("\n---- write users_table ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    users_table.write.parquet(output_data + "data_lake/users_table_parquet",
                              mode="overwrite")

    # create timestamp column from original timestamp column
    print("\n---- log df w/ mutates ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    get_timestamp = udf(lambda x: x / 1000.0)
    log_df = log_df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    log_df = log_df.withColumn('start_time', get_datetime('ts'))

    # extract columns to create time table
    print("\n---- time_table ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    time_table = log_df \
        .select('start_time') \
        .dropDuplicates() \
        .withColumn('hour',    hour(log_df.start_time)) \
        .withColumn('day',     dayofmonth(log_df.start_time)) \
        .withColumn('week',    weekofyear(log_df.start_time)) \
        .withColumn('month',   month(log_df.start_time)) \
        .withColumn('year',    year(log_df.start_time)) \
        .withColumn('weekday', dayofweek(log_df.start_time))

    # write time table to parquet files partitioned by year and month
    print("\n---- write time_table ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    time_table.write.parquet(output_data + "data_lake/time_table_parquet",
                             mode="overwrite", partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    print("\n---- read song_df ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    song_df = spark.read.parquet(output_data + "data_lake/songs_table_parquet") \
        .drop('year')

    # extract columns from joined song and log datasets to create songplays table 
    print("\n---- songplays_table ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    songplays_table = log_df \
        .withColumnRenamed('song', 'title') \
        .withColumnRenamed('artist', 'artist_name') \
        .withColumnRenamed('length', 'duration') \
        .select([
            'start_time',
            'user_id',
            'level',
            'session_id',
            'location',
            'user_agent',
            'title',
            'artist_name',
            'duration'
        ]) \
        .join(song_df, ['title', 'artist_name', 'duration']) \
        .drop('title', 'artist_name', 'duration') \
        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays_table to parquet files
    print("\n---- write songplays_table ----", datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "\n")
    songplays_table.write.parquet(output_data + "data_lake/songplays_table_parquet",
                                  mode="overwrite", partitionBy=['year', 'month'])


def main():
    """
    Driver method to use SparkSession object process data from input_data S3 bucket to output_data S3 bucket
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://aws-logs-164557480116-us-west-2/elasticmapreduce/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()
