from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, round as spark_round, explode, col
import time

def run_gold():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("03-gold-layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    print("=== GOLD LAYER ===")

    df_silver = spark.read.format("delta").load("/home/jovyan/work/data/silver/food_clean")
    print("Silver baris:", df_silver.count())

    # Agregasi per kategori
    df_gold_category = df_silver.groupBy("categories") \
        .agg(
            count("*").alias("jumlah_produk"),
            spark_round(avg("energy_kcal_100g"), 2).alias("avg_energy_kcal"),
            spark_round(avg("fat_100g"), 2).alias("avg_fat_g"),
            spark_round(avg("sugars_100g"), 2).alias("avg_sugars_g"),
            spark_round(avg("proteins_100g"), 2).alias("avg_proteins_g"),
            spark_round(avg("salt_100g"), 2).alias("avg_salt_g")
        ) \
        .filter("jumlah_produk >= 10") \
        .orderBy("jumlah_produk", ascending=False)

    # Agregasi per negara
    df_gold_country = df_silver \
        .withColumn("country", explode(col("countries_tags"))) \
        .groupBy("country") \
        .agg(
            count("*").alias("jumlah_produk"),
            spark_round(avg("energy_kcal_100g"), 2).alias("avg_energy_kcal"),
            spark_round(avg("fat_100g"), 2).alias("avg_fat_g"),
            spark_round(avg("sugars_100g"), 2).alias("avg_sugars_g"),
            spark_round(avg("proteins_100g"), 2).alias("avg_proteins_g"),
            spark_round(avg("salt_100g"), 2).alias("avg_salt_g")
        ) \
        .filter("jumlah_produk >= 10") \
        .orderBy("jumlah_produk", ascending=False)

    # Distribusi NOVA & Eco-Score
    df_nova_dist = df_silver.groupBy("nova_group") \
        .agg(count("*").alias("jumlah_produk")) \
        .orderBy("nova_group")

    df_eco_dist = df_silver.groupBy("environmental_score_grade") \
        .agg(count("*").alias("jumlah_produk")) \
        .orderBy("environmental_score_grade")

    # Simpan ke Delta Lake
    start = time.time()

    df_gold_category.write.format("delta").mode("overwrite") \
        .save("/home/jovyan/work/data/gold/nutrition_by_category")
    df_gold_country.write.format("delta").mode("overwrite") \
        .save("/home/jovyan/work/data/gold/nutrition_by_country")
    df_nova_dist.write.format("delta").mode("overwrite") \
        .save("/home/jovyan/work/data/gold/nova_distribution")
    df_eco_dist.write.format("delta").mode("overwrite") \
        .save("/home/jovyan/work/data/gold/eco_distribution")

    elapsed = time.time() - start
    print(f"Gold Layer tersimpan. Waktu: {elapsed:.1f} detik")
    print("Kategori:", df_gold_category.count())
    print("Negara:", df_gold_country.count())

    spark.stop()
    print("=== GOLD LAYER SELESAI ===")

if __name__ == "__main__":
    run_gold()