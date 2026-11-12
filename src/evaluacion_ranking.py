
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

# Entrenamiento: 2004–2022
train_base = df_ml_base.filter(
    (F.col("anio") >= 2004) &
    (F.col("anio") <= 2022)
)

# Validacion: variables de 2023 -> objetivo en 2024
validacion_base = df_ml_base.filter(
    F.col("anio") == 2023
)

print("Train:", train_base.count())
print("Validacion:", validacion_base.count())

print("\nClases train:")
train_base.groupBy(
    "target_emergencia_12m"
).count().orderBy(
    "target_emergencia_12m"
).show()

print("Clases validacion:")
validacion_base.groupBy(
    "target_emergencia_12m"
).count().orderBy(
    "target_emergencia_12m"
).show()

lr_base = LogisticRegression(
    featuresCol="features_base",
    labelCol="target_emergencia_12m",
    weightCol="peso_clase",
    maxIter=50,
    regParam=0.0,
    elasticNetParam=0.0
)

# Peso calculado exclusivamente con entrenamiento
conteos = {
    float(fila["target_emergencia_12m"]): fila["count"]
    for fila in train_base.groupBy(
        "target_emergencia_12m"
    ).count().collect()
}

peso_positivo = conteos[0.0] / conteos[1.0]

print("Peso positivo:", round(peso_positivo, 4))

train_ponderado = train_base.withColumn(
    "peso_clase",
    F.when(
        F.col("target_emergencia_12m") == 1.0,
        F.lit(peso_positivo)
    ).otherwise(F.lit(1.0))
)

modelo_base = lr_base.fit(train_ponderado)

pred_base = modelo_base.transform(validacion_base)

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

print("\n=== MÉTRICAS MODELO PONDERADO ===")
print("Accuracy :", round(accuracy, 4))
print("Precision:", round(precision, 4))
print("Recall   :", round(recall, 4))
print("F1       :", round(f1, 4))
print("F2       :", round(f2, 4))
print("PR-AUC   :", round(pr_auc, 4))

# ==========================================
# MODELO DE REFERENCIA: SIEMPRE SIN EMERGENCIA
# ==========================================

pred_ingenua = validacion_base.withColumn(
    "prediccion_ingenua",
    F.lit(0.0)
)

resumen = pred_ingenua.agg(
    F.count("*").alias("total"),
    F.sum(
        F.when(
            F.col("target_emergencia_12m") == 1.0, 1
        ).otherwise(0)
    ).alias("positivos"),
    F.sum(
        F.when(
            F.col("target_emergencia_12m") == 0.0, 1
        ).otherwise(0)
    ).alias("negativos")
).first()

total = resumen["total"]
positivos = resumen["positivos"]
negativos = resumen["negativos"]

accuracy_ingenua = negativos / total

print("\n=== MODELO DE REFERENCIA INGENUO ===")
print("Total:", total)
print("TP:", 0)
print("FN:", positivos)
print("FP:", 0)
print("TN:", negativos)

print("\n=== METRICAS REFERENCIA ===")
print("Accuracy :", round(accuracy_ingenua, 4))
print("Precision:", 0.0)
print("Recall   :", 0.0)
print("F1       :", 0.0)
print("F2       :", 0.0)

print("\n=== COMPARACION ===")
print("Accuracy original:", round(accuracy, 4))
print("Accuracy ingenua :", round(accuracy_ingenua, 4))
print("Diferencia      :", round(accuracy - accuracy_ingenua, 4))

# Comparacion de umbrales sobre VALIDACION
pares = [
    (int(f.target_emergencia_12m), float(f.probability[1]))
    for f in pred_base.select(
        "target_emergencia_12m", "probability"
    ).collect()
]

print("\n=== COMPARACION DE UMBRALES ===")
print("Umbral | TP | FP | FN | TN | Precision | Recall | F2")

for umbral in [0.30, 0.40, 0.50, 0.55, 0.60, 0.65, 0.70, 0.80]:
    tp = sum(1 for y, p in pares if y == 1 and p >= umbral)
    fp = sum(1 for y, p in pares if y == 0 and p >= umbral)
    fn = sum(1 for y, p in pares if y == 1 and p < umbral)
    tn = sum(1 for y, p in pares if y == 0 and p < umbral)

    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    f2 = (
        5 * precision * recall / (4 * precision + recall)
        if 4 * precision + recall else 0
    )

    print(
        f"{umbral:.2f} | {tp} | {fp} | {fn} | {tn} | "
        f"{precision:.4f} | {recall:.4f} | {f2:.4f}"
    )


# Evaluacion del ranking mensual de distritos
from collections import defaultdict

grupos = defaultdict(list)

for fila in pred_base.select(
    "mes_num",
    "target_emergencia_12m",
    "probability"
).collect():
    mes = int(fila["mes_num"])
    real = int(fila["target_emergencia_12m"])
    score = float(fila["probability"][1])
    grupos[mes].append((real, score))

total_positivos = sum(
    real
    for registros in grupos.values()
    for real, score in registros
)

print("\n=== RANKING MENSUAL DE DISTRITOS ===")
print("Top K | Alertas | TP | FP | Precision | Recall")

for k in [5, 10, 20]:
    seleccionados = []

    for mes, registros in sorted(grupos.items()):
        ordenados = sorted(
            registros,
            key=lambda x: x[1],
            reverse=True
        )
        seleccionados.extend(ordenados[:k])

    tp = sum(real for real, score in seleccionados)
    alertas = len(seleccionados)
    fp = alertas - tp

    precision = tp / alertas if alertas else 0
    recall = tp / total_positivos if total_positivos else 0

    print(
        f"{k} | {alertas} | {tp} | {fp} | "
        f"{precision:.4f} | {recall:.4f}"
    )

spark.stop()
