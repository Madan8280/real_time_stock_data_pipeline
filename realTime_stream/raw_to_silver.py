from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("RawToSilver") \
    .config("spark.hadoop.fs.s3a.endpoint","http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key","minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoopfs.s3a.path.style.access","true") \
    .getOrCreate()


df = spark.read.csv("s3a://raw/stock-prices/", header=False, inferSchema=True) \
    .toDF("symbol","timestamp","open","high","low","close","volume")

clean = df.dropDuplicates(["symbol", "timestamp"]) \
    .filter(col("close").isNotNull()) \
    .filter(col("volume") >0)

clean.write.mode("append").parquate("s3a://silver/stock-prices/")
print(f"wrote {clean.count()} clean rows to silver")
