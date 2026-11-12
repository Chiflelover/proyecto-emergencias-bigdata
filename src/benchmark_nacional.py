import argparse
import statistics
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--factor", type=int, choices=[1, 10, 100], required=True)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName(f"BenchmarkNacional_{args.factor}x")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

origen = "/proyecto_emergencias/processed/emergencias_nacional"
destino = f"/proyecto_emergencias/benchmark/nacional_{args.factor}x"

base = spark.read.parquet(origen).select(
    "codigo_sinpad",
    "departamento",
    "anio",
    "peligro",
    "afectados",
    "damnificados"
)

# Crear replicas identificables para el experimento
replicas = spark.range(args.factor).select(
    F.col("id").alias("replica_id")
)

ampliado = base.crossJoin(F.broadcast(replicas))

print(f"\n=== PREPARACION {args.factor}X ===", flush=True)

inicio = time.perf_counter()

ampliado.repartition(8).write.mode("overwrite").parquet(
    destino + "/entrada"
)

tiempo_preparacion = time.perf_counter() - inicio

datos = spark.read.parquet(destino + "/entrada")
registros = datos.count()
esperados = 142139 * args.factor

assert registros == esperados, (
    f"Registros incorrectos: {registros} != {esperados}"
)

print("Registros verificados:", registros, flush=True)
print("Tiempo preparacion:", round(tiempo_preparacion, 3), flush=True)

# Medir el procesamiento sobre datos ya materializados
tiempos = []

for ejecucion in range(1, 4):
    inicio = time.perf_counter()

    df = spark.read.parquet(destino + "/entrada")

    procesado = (
        df
        .withColumn(
            "afectados_limpios",
            F.coalesce(F.col("afectados"), F.lit(0))
        )
        .withColumn(
            "damnificados_limpios",
            F.coalesce(F.col("damnificados"), F.lit(0))
        )
        .groupBy("departamento", "anio", "peligro")
        .agg(
            F.count("*").alias("emergencias"),
            F.sum("afectados_limpios").alias("total_afectados"),
            F.sum("damnificados_limpios").alias("total_damnificados")
        )
    )

    procesado.write.mode("overwrite").parquet(
        destino + f"/resultado_{ejecucion}"
    )

    duracion = time.perf_counter() - inicio
    tiempos.append(duracion)

    print(
        f"Ejecucion {ejecucion}: {duracion:.3f} segundos",
        flush=True
    )

print(f"\n=== RESULTADOS BENCHMARK {args.factor}X ===", flush=True)
print("Registros:", registros, flush=True)
print("Tiempos:", [round(t, 3) for t in tiempos], flush=True)
print("Mediana:", round(statistics.median(tiempos), 3), flush=True)

spark.stop()
