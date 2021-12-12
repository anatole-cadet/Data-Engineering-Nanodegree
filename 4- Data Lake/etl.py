import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, dayofweek ,weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """This function is for creating a spark session.
    return:
        spark(String): spark session informations.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """This function is for getting songs data on s3; process data and load them back to s3.
    Args:
        spark(String)        : This is a variable SparkSession
        input_data(String)   : The path where the songs data are in s3  
        output_data(String)  : The path where to load data after processing
    """
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration').dropDuplicates().createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    query_songs_table = spark.sql('''
        SELECT song_id, artist_id, title,year,duration FROM songs
    ''')
    query_songs_table.write.partitionBy("year","artist_id").parquet(output_data + "songs_table/",'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude').dropDuplicates().createOrReplaceTempView('artists')
    
    # write artists table to parquet files
    artists_table = spark.sql(''' SELECT artist_id,artist_name name,artist_location as location,artist_longitude as longitude, artist_latitude as latitude FROM artists''')\
        .write.parquet(output_data + "artists_table/", 'overwrite')


def process_log_data(spark, input_data, output_data):
    """This function is for getting logs data on s3; process logs data and load them back to s3.
    Args:
        spark(String)        : This is a variable SparkSession
        input_data(String)   : The path where the logs data are in s3  
        output_data(String)  : The path where to load data after processing
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    song_plays = df.select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song'),
        col('artist'),
        col('sessionId').alias('session_id'),
        col('location'), 
        col('userAgent').alias('user_agent')).filter("page =='NextSong'")
    print(df.show(5))

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            col('gender'),
                            col('level')).filter("page == 'NextSong'").createOrReplaceTempView("users_table")
    
    # write users table to parquet files
    spark.sql(''' SELECT user_id, first_name, last_name, gender,level FROM users_table''').write.parquet(output_data + "users_table/", "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda t: datetime.fromtimestamp(t/1000))
    df = df.withColumn("start_time", get_timestamp(col("ts")))
    df = df.withColumn("hour", hour("start_time"))\
            .withColumn("day", dayofmonth("start_time"))\
            .withColumn("week",weekofyear("start_time"))\
            .withColumn("month",month("start_time"))\
            .withColumn("year", year("start_time"))\
            .withColumn("weekday",dayofweek("start_time"))
 
    
    # extract columns to create time table
    time_table = df.select(
        col('start_time'),col('hour'), col('day'), col('week'),col('month'), col('year'), col('weekday')
    ).filter("page == 'NextSong'").createOrReplaceTempView("time")
    
    # write time table to parquet files partitioned by year and month
    spark.sql(''' SELECT start_time, hour, day, week, month, year, weekday FROM time''')\
        .write.partitionBy("year","month")\
        .parquet(output_data + "time_table/", 'overwrite')

    # read in song data to use for songplays table
    song_data = spark.read.json(input_data + "song_data/A/A/A/*.json").createOrReplaceTempView("songs")
    


    # extract columns from joined song and log datasets to create songplays table
    log_dataset = df.createOrReplaceTempView("logs")
    songplays_table = spark.sql('''
        SELECT
            monotonically_increasing_id() AS songplay_id
            ,l.start_time
            ,l.userId AS user_id
            ,l.level
            ,s.song_id
            ,s.artist_id
            ,l.sessionId AS session_id
            ,s.artist_location AS location
            ,l.userAgent AS user_agent
            ,l.year
            ,l.month
        FROM songs s
        JOIN logs l ON s.artist_name = l.artist
        AND s.title = l.song
        AND s.duration = l.length        
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songs_plays_table/", 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-emr-resources-617920907329-us-west-2/notebooks/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
