from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

spark.conf.set("viewsEnabled", "true")
spark.conf.set("materializationDataset", "spotify")

sql_query = "SELECT count(*) FROM `sacred-alloy-375819.spotify.spotify_data`"

df = spark.read.format("bigquery").load(sql_query)
df.show()
