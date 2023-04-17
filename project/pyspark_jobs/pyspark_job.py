from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.conf.set("viewsEnabled", "true")
spark.conf.set("materializationDataset", "spotify")


transform_data_query1 = """
    SELECT count(*) as top_count, CONCAT(artist, ' - ', title) as song FROM `sacred-alloy-375819.spotify.spotify_data`
    where region = "Global" and rank = 1 and chart = "top200" group by title, artist, url order by count(*) desc;
    """

data1 = spark.read.format('bigquery').option('query', transform_data_query1).load()
data1.write.format('bigquery') \
  .option('table', 'sacred-alloy-375819.spotify.spotify_transform_data1') \
  .option('writeMethod', 'direct') \
  .mode("overwrite") \
  .save()


transform_data_query2 = """
    SELECT count(*) as top_count, artist FROM `sacred-alloy-375819.spotify.spotify_data`
    where region = "Global" and rank = 1 and chart = "top200" group by artist order by count(*) desc;
    """

data2 = spark.read.format('bigquery').option('query', transform_data_query2).load()
data2.write.format('bigquery') \
  .option('table', 'sacred-alloy-375819.spotify.spotify_transform_data2') \
  .option('writeMethod', 'direct') \
  .mode("overwrite") \
  .save()


transform_data_query3 = """
    SELECT 
        count(*) as top_count, 
        CONCAT(artist, ' - ', title) as song, 
        artist, 
        title, 
        region 
    FROM `sacred-alloy-375819.spotify.spotify_data`
    where rank = 1 and chart = "top200" 
    group by title, artist, url, region 
    order by count(*) desc;
    """

data3 = spark.read.format('bigquery').option('query', transform_data_query3).load()
data3.write.format('bigquery') \
  .option('table', 'sacred-alloy-375819.spotify.spotify_transform_data3') \
  .option('writeMethod', 'direct') \
  .mode("overwrite") \
  .save()

transform_data_query4 = """
    SELECT count(*) as top_count, artist, region FROM `sacred-alloy-375819.spotify.spotify_data`
    where rank = 1 and chart = "top200" group by artist, region order by count(*) desc;
    """

data4 = spark.read.format('bigquery').option('query', transform_data_query4).load()
data4.write.format('bigquery') \
  .option('table', 'sacred-alloy-375819.spotify.spotify_transform_data4') \
  .option('writeMethod', 'direct') \
  .mode("overwrite") \
  .save()
