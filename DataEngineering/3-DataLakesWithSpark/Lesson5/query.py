import configparser
import os
from pyspark.sql import SparkSession

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


def run_query_1(spark, s3_data_lake_key):
    """
    Performs a join between the fact table (df_songplays) and a dimension table (df_songs)

    :param spark: SparkSession object
    :param s3_data_lake_key: S3 bucket URI string to read json files from
    :return: result DataFrame from join
    """
    df_songs = spark.read.parquet(s3_data_lake_key + "songs_table_parquet")
    df_songplays = spark.read.parquet(s3_data_lake_key + "songplays_table_parquet")

    df_join = df_songplays \
        .select(['songplay_id', 'start_time', 'song_id']) \
        .join(df_songs, ['song_id'])

    return df_join


def main():
    """
    Driver method to use SparkSession object process data from input_data S3 bucket to output_data S3 bucket
    """
    spark = create_spark_session()
    data_lake_bucket = "s3://aws-logs-164557480116-us-west-2/elasticmapreduce/data_lake/"

    df_query_1 = run_query_1(spark, data_lake_bucket)
    df_query_1.printSchema()
    print(df_query_1.show(10))

    spark.stop()


if __name__ == "__main__":
    main()
