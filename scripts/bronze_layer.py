from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import time

def run_bronze():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("01-bronze-ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    print("=== BRONZE LAYER ===")
    print("Spark version:", spark.version)

    start = time.time()
    df = spark.read.parquet("/home/jovyan/work/food.parquet")
    print("Jumlah kolom:", len(df.columns))

    df.withColumn("ingestion_timestamp", current_timestamp()) \
      .write \
      .format("delta") \
      .mode("overwrite") \
      .option("maxRecordsPerFile", 250000) \
      .save("/home/jovyan/work/data/bronze/food_raw")

    elapsed = time.time() - start
    throughput = 4487169 / elapsed
    print(f"Bronze Layer tersimpan. Waktu: {elapsed:.1f} detik")
    print(f"Throughput: {throughput:,.0f} baris/detik")

    df_bronze = spark.read.format("delta").load("/home/jovyan/work/data/bronze/food_raw")
    print("Validasi — Jumlah baris:", df_bronze.count())
    print("Validasi — Jumlah kolom:", len(df_bronze.columns))

    spark.stop()
    print("=== BRONZE LAYER SELESAI ===")

if __name__ == "__main__":
    run_bronze()