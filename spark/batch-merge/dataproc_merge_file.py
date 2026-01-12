import sys
from pyspark.sql import SparkSession

RAW_PATH = sys.argv[1]
MERGED_PATH = sys.argv[2]

spark = SparkSession.builder \
    .appName("Merge Parquet") \
    .getOrCreate()

df = spark.read.parquet(RAW_PATH)
if df.count() > 0:
    print(f"Đang gộp {df.count()} dòng dữ liệu...")
    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(MERGED_PATH)
    print(f"Gộp file thành công tại: {MERGED_PATH}")
else:
    print("Không tìm thấy dữ liệu để gộp.")

spark.stop()
