import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load data from song_data dataset and extract columns
    for songs and artist tables and write the data into parquet
    files which will be loaded on s3.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_data.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data,"songs_table"), partitionBy = ['year', 'artist_id'])
                 
    # extract columns to create artists table
    artists_table = song_data.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists_table"))


def process_log_data(spark, input_data, output_data):
    """
    Load data from log_data dataset and extract columns
    for users and time tables, reads both the log_data and song_data
    datasets and extracts columns for songplays table with the data.
    It writes the data into parquet files which will be loaded on s3.
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = log_data.filter(log_data.page == 'NextSong')

    # extract columns for users table    
    user_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data, "user_table"))

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = df.withColumn('timestamp', col('ts')/1000)
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = df.withColumn('datetime', date_format(from_unixtime(col('ts')/1000), 'yyyy-MM-dd HH:mm:ss'))
    
    # extract columns to create time table
    time_table = df.select(col('datetime').alias('start_time'), hour('datetime').alias('hour'), dayofmonth('datetime').alias('day'), weekofyear('datetime').alias('week'), month('datetime').alias('month'), year('datetime').alias('year'), dayofweek('datetime').alias('weekday')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table"), partitionBy = ['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(output_data,"songs_table/*/*/*.parquet"))
    time_table_sub = time_table.select('start_time', 'month')
    df = df.join(time_table_sub, df.datetime == time_table_sub.start_time)
    df = df.join(song_df, df.song == song_df.title)
    df = df.withColumn("songplay_id", monotonically_increasing_id())
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select('songplay_id', col('datetime').alias('start_time'), 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', 'year', 'month').dropDuplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays_table"), partitionBy = ['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-ag-bucket1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
