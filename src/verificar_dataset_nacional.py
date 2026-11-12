from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "VerificacionSINPADNacional"
).getOrCreate()

ruta = "/proyecto_emergencias/processed/emergencias_nacional"

df = spark.read.parquet(ruta)

print("\n=== VERIFICACION DATASET NACIONAL ===")
print("Registros:", df.count())
print("Columnas:", len(df.columns))

print("\n=== ESQUEMA ===")
df.printSchema()

spark.stop()
