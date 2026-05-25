from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit, lower, size, array_contains
import time

def run_silver():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("02-silver-layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    print("=== SILVER LAYER ===")

    # Baca Bronze Layer
    df_bronze = spark.read.format("delta").load("/home/jovyan/work/data/bronze/food_raw")
    print("Bronze baris:", df_bronze.count())

    # Filter nova_group IS NOT NULL
    df_filtered = df_bronze.filter(df_bronze.nova_group.isNotNull())
    print("Setelah filter:", df_filtered.count())

    # UNNEST nutriments
    df_pivot = df_filtered.select(
        col("*"),
        *[
            F.expr(f"""
                aggregate(
                    filter(nutriments, x -> x.name = '{n}'),
                    cast(null as float),
                    (acc, x) -> x.`100g`
                )
            """).alias(f"{n.replace('-', '_')}_100g")
            for n in ['energy-kcal', 'fat', 'sugars', 'proteins', 'salt']
        ]
    ).drop("nutriments")

    # Filter outlier + median imputation
    nutrisi_cols = ['energy_kcal_100g', 'fat_100g', 'sugars_100g', 'proteins_100g', 'salt_100g']
    df_clean = df_pivot
    for c in nutrisi_cols:
        df_clean = df_clean.withColumn(c,
            when((col(c) >= 0) & (col(c) <= 1000), col(c)).otherwise(None)
        )

    medians = {}
    for c in nutrisi_cols:
        median_val = df_clean.approxQuantile(c, [0.5], 0.01)[0]
        medians[c] = median_val
        df_clean = df_clean.withColumn(c,
            when(col(c).isNull(), lit(median_val)).otherwise(col(c))
        )

    print("Median nutrisi:", medians)

    # Encoding + normalisasi + feature engineering
    df_clean = df_clean.withColumn("nutriscore_encoded",
        when(col("nutriscore_grade") == "a", 1)
        .when(col("nutriscore_grade") == "b", 2)
        .when(col("nutriscore_grade") == "c", 3)
        .when(col("nutriscore_grade") == "d", 4)
        .when(col("nutriscore_grade") == "e", 5)
        .otherwise(None)
    )
    df_clean = df_clean.withColumn("brands", lower(col("brands")))
    df_clean = df_clean \
        .withColumn("brands", when(col("brands").isNull(), lit("unknown")).otherwise(col("brands"))) \
        .withColumn("categories", when(col("categories").isNull(), lit("unknown")).otherwise(col("categories")))
    df_clean = df_clean.withColumn("additives_count",
        when(col("additives_tags").isNull(), 0).otherwise(size(col("additives_tags")))
    )
    df_clean = df_clean.withColumn("has_palm_oil",
        when(array_contains(col("ingredients_analysis_tags"), "en:palm-oil"), 1).otherwise(0)
    )
    df_clean = df_clean.withColumn("is_vegan",
        when(array_contains(col("ingredients_analysis_tags"), "en:vegan"), 1).otherwise(0)
    )
    df_clean = df_clean.withColumn("is_vegetarian",
        when(array_contains(col("ingredients_analysis_tags"), "en:vegetarian"), 1).otherwise(0)
    )

    # Simpan ke Delta Lake
    start = time.time()
    df_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("nova_group") \
        .option("maxRecordsPerFile", 250000) \
        .save("/home/jovyan/work/data/silver/food_clean")

    elapsed = time.time() - start
    print(f"Silver Layer tersimpan. Waktu: {elapsed:.1f} detik")

    df_silver = spark.read.format("delta").load("/home/jovyan/work/data/silver/food_clean")
    print("Validasi — Jumlah baris:", df_silver.count())
    print("Validasi — Jumlah kolom:", len(df_silver.columns))

    spark.stop()
    print("=== SILVER LAYER SELESAI ===")

if __name__ == "__main__":
    run_silver()