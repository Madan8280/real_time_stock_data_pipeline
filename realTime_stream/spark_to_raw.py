from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType


spark = SparkSession.builder \
.appName("kafkaToRaw") \
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
.config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
.config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
.config("spark.hadoop.fs.s3a.path.style.access", "true") \
.getOrCreate()


schema = StructType() \
    .add("symbol",StringType()) \
    .add("timestamp", StringType()) \
    .add("open", DoubleType()) \
    .add("high", DoubleType()) \
    .add("low",DoubleType()) \
    .add("close",DoubleType()) \
    .add("volume",LongType())


df = (spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "stock-prices") \
    .option("startingOffsets", "earliest") \
    .load()
)

# parsed = (df.select(from_json(col("value").cast("string"),
#             schema).alias("data").select("data.*"))
#             )
parsed = (
    df.select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
)



query = parsed.writeStream \
        .format("csv") \
        .option("path", "s3a://raw/stock-prices/") \
        .option("checkpointLocation", "s3a://raw/checkpoints/") \
        .outputMode("append") \
        .start()


query.awaitTermination()

