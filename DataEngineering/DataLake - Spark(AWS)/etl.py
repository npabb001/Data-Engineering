import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format



#config = configparser.ConfigParser()
#config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=''
os.environ['AWS_SECRET_ACCESS_KEY']=''


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet( output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude' ).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
    #print(log_data);

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')
    print(df.ts)

    # extract columns for users table    
    users_table = df.select('userid', 'firstname', 'lastname', 'gender', 'level' ).distinct()
    
    # write users table to parquet files
    users_table.write.parquet( output_data + "users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df1 = df.withColumn('datetime',get_timestamp(df.ts))
    print(df1.head(5))
    print('done with using UDF----------------------------------------------------------------------------------------------')
    print('done with using UDF----------------------------------------------------------------------------------------------')
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    df1.createOrReplaceTempView("time")
    # extract columns to create time table
    time_table = spark.sql(""" select distinct ts as start_time, extract(hour from datetime) as hour, extract(day from datetime) as time, extract(week from datetime) as week, extract(month from datetime) as month, extract(year from datetime) as year from time """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet( output_data + "songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df1, df1.song == song_df.title).selectExpr('datetime as start_time', 'userId as user_id', 'level', 'song_id', 'artist_id', 'sessionId as session_id', 'location', "userAgent as user_agent")
    songplays_table = songplays_table.withColumn("songplays_id", F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakevamsi/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
