from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import time

def run_ml():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("04-ml-nova-classifier") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    print("=== ML LAYER ===")

    df_silver = spark.read.format("delta").load("/home/jovyan/work/data/silver/food_clean")

    feature_cols = [
        'energy_kcal_100g', 'fat_100g', 'sugars_100g',
        'proteins_100g', 'salt_100g', 'additives_count',
        'nutriscore_encoded', 'has_palm_oil', 'is_vegan', 'is_vegetarian'
    ]

    df_ml = df_silver.select(feature_cols + ['nova_group']) \
        .filter("nova_group IS NOT NULL") \
        .na.drop()

    print("ML dataset:", df_ml.count(), "baris")

    df_ml = df_ml.withColumn("label", col("nova_group") - 1)
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_ml = assembler.transform(df_ml)

    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    print("Train:", train_df.count(), "| Test:", test_df.count())

    # Class weight
    total = train_df.count()
    weights = train_df.groupBy("label").count().collect()
    weight_map = {row["label"]: total / (4 * row["count"]) for row in weights}

    train_weighted = train_df
    for label, weight in weight_map.items():
        train_weighted = train_weighted.withColumn(
            "class_weight",
            when(col("label") == label, weight).otherwise(
                col("class_weight") if "class_weight" in train_weighted.columns else lit(weight)
            )
        )

    # Train Random Forest
    rf = RandomForestClassifier(
        featuresCol="features", labelCol="label",
        weightCol="class_weight", numTrees=50, maxDepth=10, seed=42
    )

    start = time.time()
    rf_model = rf.fit(train_weighted)
    elapsed = time.time() - start
    print(f"Model trained. Waktu: {elapsed:.1f} detik")

    # Evaluasi
    predictions = rf_model.transform(test_df)
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )

    f1_macro = evaluator_f1.evaluate(predictions)
    accuracy = evaluator_acc.evaluate(predictions)
    print(f"F1-score macro: {f1_macro:.4f} ({f1_macro*100:.2f}%)")
    print(f"Accuracy: {accuracy:.4f} ({accuracy*100:.2f}%)")

    for i, nova in enumerate([1, 2, 3, 4]):
        ev = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction",
            metricName="fMeasureByLabel", metricLabel=float(i)
        )
        print(f"  NOVA {nova} F1: {ev.evaluate(predictions):.4f}")

    # Simpan model
    rf_model.write().overwrite().save("/home/jovyan/work/data/rf_nova_model")
    print("Model disimpan.")

    spark.stop()
    print("=== ML LAYER SELESAI ===")

if __name__ == "__main__":
    run_ml()