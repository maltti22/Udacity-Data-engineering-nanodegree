import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as StrT, StructField as StrFld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        This function creates or retrieves a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
        This function gets the data from the s3 bucket and creates the 
        artists and songs tablesand then again loaded back to S3
        
        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files with the songs metadata
            output_data : S3 bucket were dimensional tables in parquet format will be stored
    """
    
    print("--- Song_Data starts ---")
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    #song_data = input_data + 'song-data/A/A/A'
    
    songSchema =  StructType([
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", FloatType(), True),
        StructField("year", IntegerType(), False)
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema)
    #df.schema


    # extract columns to create songs table
    song_columns = ["title", "artist_id","year", "duration"]

    
    # write songs table to parquet files partitioned by year and artist
    print("--- write songs table to parquet files ---")
    songs_table = df.select(song_columns).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    songs_table.show(10)
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/', mode="overwrite")
    
   

    print("--- write artist table to parquet files ---")
    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    artists_table.show(10)

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")
    


def process_log_data(spark, input_data, output_data):
    
    """
    Description:
            This function processes the event log file and extract data for table time, users and songplays from it.
            
    Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files with the events data
            output_data : S3 bucket were dimensional tables in parquet format will be stored
    """
    
    print("--- Log_Data starts ---")
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates()
    
    print("--- write user table to parquet files ---")
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode="overwrite")


    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
 
   
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time_table/', mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/*')
    artists_df = spark.read.parquet(output_data + 'artists/*')

    # extract columns from joined song and log datasets to create songplays table
    df = df.join(song_df, (song_df.title == df.song) & (song_df.artist_name == df.artist))
    df = df.withColumn('songplay_id', monotonically_increasing_id()) 
    
    songplays_table = df['songplay_id','start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"    
    output_data = "s3a://data-lake-output-data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
