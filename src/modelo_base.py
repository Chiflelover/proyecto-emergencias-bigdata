
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--panel-path", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName(
    "ModeloBaseEmergenciasS7"
).getOrCreate()

df_panel = spark.read.parquet(args.panel_path)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = (
    Window
    .partitionBy("ubigeo")
    .orderBy("anio", "mes_num")
)

df_base_modelo = (
    df_panel

    # Emergencias del mes anterior
    .withColumn(
        "emergencias_mes_anterior",
        F.lag("cantidad_emergencias_mes", 1).over(w)
    )

    # Mismo mes del año anterior
    .withColumn(
        "emergencias_mismo_mes_anio_anterior",
        F.lag("cantidad_emergencias_mes", 12).over(w)
    )

    # Lo que ocurre 12 meses después
    .withColumn(
        "emergencias_12m_despues",
        F.lead("cantidad_emergencias_mes", 12).over(w)
    )

    # Estacionalidad
    .withColumn(
        "mes_sin",
        F.sin(
            2 * F.lit(3.141592653589793)
            * F.col("mes_num") / 12
        )
    )

    .withColumn(
        "mes_cos",
        F.cos(
            2 * F.lit(3.141592653589793)
            * F.col("mes_num") / 12
        )
    )

    # Variable objetivo binaria
    .withColumn(
        "target_emergencia_12m",
        F.when(
            F.col("emergencias_12m_despues").isNull(),
            F.lit(None)
        )
        .when(
            F.col("emergencias_12m_despues") > 0,
            F.lit(1.0)
        )
        .otherwise(F.lit(0.0))
    )

    # Necesitamos al menos un año anterior
    .filter(
        (F.col("anio") >= 2004) &
        F.col("target_emergencia_12m").isNotNull()
    )
)

print("Filas modelables:", df_base_modelo.count())

df_base_modelo.groupBy(
    "target_emergencia_12m"
).count().orderBy(
    "target_emergencia_12m"
).show()

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

features_base = [
    "cantidad_emergencias_mes",
    "emergencias_mes_anterior",
    "emergencias_mismo_mes_anio_anterior",
    "precipitacion_total_mm",
    "mes_sin",
    "mes_cos"
]

assembler_base = VectorAssembler(
    inputCols=features_base,
    outputCol="features_base",
    handleInvalid="skip"
)

df_ml_base = assembler_base.transform(df_base_modelo)

# Entrenamiento: 2004–2023
train_base = df_ml_base.filter(
    (F.col("anio") >= 2004) &
    (F.col("anio") <= 2023)
)

# Prueba: variables de 2024 -> objetivo en 2025
test_base = df_ml_base.filter(
    F.col("anio") == 2024
)

print("Train:", train_base.count())
print("Test:", test_base.count())

print("\nClases train:")
train_base.groupBy(
    "target_emergencia_12m"
).count().orderBy(
    "target_emergencia_12m"
).show()

print("Clases test:")
test_base.groupBy(
    "target_emergencia_12m"
).count().orderBy(
    "target_emergencia_12m"
).show()

lr_base = LogisticRegression(
    featuresCol="features_base",
    labelCol="target_emergencia_12m",
    maxIter=50,
    regParam=0.0,
    elasticNetParam=0.0
)

modelo_base = lr_base.fit(train_base)

pred_base = modelo_base.transform(test_base)

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F

confusion = (
    pred_base
    .agg(
        F.sum(
            F.when(
                (F.col("prediction") == 1.0) &
                (F.col("target_emergencia_12m") == 1.0),
                1
            ).otherwise(0)
        ).alias("TP"),

        F.sum(
            F.when(
                (F.col("prediction") == 0.0) &
                (F.col("target_emergencia_12m") == 1.0),
                1
            ).otherwise(0)
        ).alias("FN"),

        F.sum(
            F.when(
                (F.col("prediction") == 1.0) &
                (F.col("target_emergencia_12m") == 0.0),
                1
            ).otherwise(0)
        ).alias("FP"),

        F.sum(
            F.when(
                (F.col("prediction") == 0.0) &
                (F.col("target_emergencia_12m") == 0.0),
                1
            ).otherwise(0)
        ).alias("TN")
    )
    .first()
)

TP = confusion["TP"]
FN = confusion["FN"]
FP = confusion["FP"]
TN = confusion["TN"]

accuracy = (TP + TN) / (TP + TN + FP + FN)

precision = (
    TP / (TP + FP)
    if (TP + FP) > 0 else 0
)

recall = (
    TP / (TP + FN)
    if (TP + FN) > 0 else 0
)

f1 = (
    2 * precision * recall / (precision + recall)
    if (precision + recall) > 0 else 0
)

f2 = (
    5 * precision * recall / (4 * precision + recall)
    if (4 * precision + recall) > 0 else 0
)

pr_auc = BinaryClassificationEvaluator(
    labelCol="target_emergencia_12m",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
).evaluate(pred_base)

print("=== MATRIZ DE CONFUSIÓN ===")
print("TP:", TP)
print("FN:", FN)
print("FP:", FP)
print("TN:", TN)

print("\n=== MÉTRICAS MODELO BASE ===")
print("Accuracy :", round(accuracy, 4))
print("Precision:", round(precision, 4))
print("Recall   :", round(recall, 4))
print("F1       :", round(f1, 4))
print("F2       :", round(f2, 4))
print("PR-AUC   :", round(pr_auc, 4))

spark.stop()
