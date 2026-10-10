#!/usr/bin/env python3
"""RAW -> PROCESSED reproducible para La Libertad (SINPAD+COEN+CHIRPS+INEI).

Reconstruido de las transformaciones vigentes del notebook Proyecto_Emergencias_BigData.
No entrena modelos ni descarga fuentes externas. No modifica las salidas históricas
existentes: escribe en un prefijo HDFS de reproducción separado, sin sobrescribir.
"""

import argparse
import logging
import re
import sys
import unicodedata

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LOG = logging.getLogger("emergencias_pipeline")


def limpiar_nombre_columna(nombre):
    nombre = unicodedata.normalize("NFKD", nombre)
    nombre = "".join(c for c in nombre if not unicodedata.combining(c))
    nombre = re.sub(r"[^a-z0-9]+", "_", nombre.lower().strip())
    return re.sub(r"_+", "_", nombre).strip("_")


def normalizar_clave(columna):
    return F.translate(F.upper(F.trim(columna)), "ÁÉÍÓÚÜÑ", "AEIOUUN")


def revisar_columnas(df, columnas, fuente):
    faltantes = sorted(set(columnas) - set(df.columns))
    if faltantes:
        raise ValueError(f"{fuente}: columnas ausentes: {faltantes}")


def verificar_total(df, esperado, etiqueta):
    n = df.count()
    LOG.info("%s: %d filas", etiqueta, n)
    if esperado is not None and n != esperado:
        raise RuntimeError(f"{etiqueta}: se esperaban {esperado} filas, se obtuvieron {n}")
    return n


def leer_sinpad(spark, ruta):
    raw = (spark.read.option("header", True).option("inferSchema", True)
           .option("sep", ";").option("encoding", "ISO-8859-1").csv(ruta))
    renombradas = [(c, limpiar_nombre_columna(c)) for c in raw.columns]
    for origen, destino in renombradas:
        raw = raw.withColumnRenamed(origen, destino)
    raw = (raw.withColumnRenamed("codigo_de_emergencia_sinpad", "codigo_sinpad")
           .withColumnRenamed("fecha_de_la_emer", "fecha_emergencia_original")
           .withColumnRenamed("ano", "anio")
           .withColumnRenamed("dpto", "departamento")
           .withColumnRenamed("prov", "provincia")
           .withColumnRenamed("dist", "distrito"))
    revisar_columnas(raw,
                     ["codigo_sinpad", "fecha_emergencia_original", "anio",
                      "departamento", "provincia", "distrito", "peligro",
                      "afectados", "damnificados", "fallecidos", "desaparecidos",
                      "viviendas_afectadas", "viviendas_destruidas"],
                     "SINPAD")
    partes = F.split(F.col("fecha_emergencia_original"), "/")
    # Regla encontrada/corregida en notebook: 2003-2024 DD/MM/YYYY;
    # formato de 2 dígitos de 2025 MM/DD/YY.
    fecha = (F.when(F.col("fecha_emergencia_original").rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
                    F.make_date(partes[2].cast("int"), partes[1].cast("int"), partes[0].cast("int")))
             .when(F.col("fecha_emergencia_original").rlike(r"^\d{1,2}/\d{1,2}/\d{2}$"),
                   F.make_date(partes[2].cast("int") + F.lit(2000),
                               partes[0].cast("int"), partes[1].cast("int")))
             .otherwise(F.lit(None).cast("date")))
    limpio = raw.withColumn("fecha_emergencia", fecha)
    for col in ("departamento", "provincia", "distrito", "peligro",
                "tipo_de_peligro", "region_natural", "mes"):
        if col in limpio.columns:
            limpio = limpio.withColumn(col, F.upper(F.trim(F.col(col))))
    limpio = limpio.drop("fecha_emergencia_original")
    malos = limpio.filter(F.col("fecha_emergencia").isNull() |
                          (F.year("fecha_emergencia") != F.col("anio"))).count()
    if malos:
        raise RuntimeError(f"SINPAD: {malos} fechas nulas/inconsistentes; no se escribe la salida")
    return limpio


def leer_coen(spark, ruta):
    df = spark.read.option("header", True).option("inferSchema", True).csv(ruta)
    revisar_columnas(df, ["sinpad_final", "titulo", "url_pdf", "anio",
                         "metodo_match", "confianza_match"], "COEN")
    validos = (df.filter(F.col("sinpad_final").isNotNull())
               .withColumn("codigo_sinpad", F.col("sinpad_final").cast("int")))
    agrupado = validos.groupBy("codigo_sinpad").agg(
        F.count("*").alias("cantidad_reportes_coen"),
        F.collect_set("titulo").alias("titulos_coen"),
        F.collect_set("url_pdf").alias("urls_pdf_coen"),
        F.collect_set("anio").alias("anios_reportes_coen"),
        F.collect_set("metodo_match").alias("metodos_match_coen"),
        F.collect_set("confianza_match").alias("confianzas_match_coen"),
    )
    return df, agrupado


def leer_chirps(spark, ruta):
    df = spark.read.option("header", True).option("inferSchema", True).csv(ruta)
    revisar_columnas(df, ["ubigeo", "provincia", "distrito", "anio", "mes",
                         "precipitacion_total_mm", "precipitacion_promedio_diaria_mm",
                         "precipitacion_maxima_diaria_mm"], "CHIRPS")
    mensual = (df.withColumn("mes_num", F.col("mes").cast("int"))
               .withColumn("provincia_key", normalizar_clave(F.col("provincia")))
               .withColumn("distrito_key", normalizar_clave(F.col("distrito"))))
    duplicados = (mensual.groupBy("ubigeo", "anio", "mes_num")
                  .count().filter(F.col("count") > 1).limit(1).count())
    if duplicados:
        raise RuntimeError("CHIRPS: existen distrito-meses duplicados")
    return mensual


def leer_inei(spark, ruta):
    df = spark.read.option("header", True).option("inferSchema", True).csv(ruta)
    revisar_columnas(df, ["provincia", "distrito", "anio", "poblacion"], "INEI")
    norm = (df.withColumn("provincia_key", normalizar_clave(F.col("provincia")))
            .withColumn("distrito_key", normalizar_clave(F.col("distrito")))
            .select("provincia_key", "distrito_key", "anio", "poblacion"))
    dup = (norm.groupBy("provincia_key", "distrito_key", "anio")
           .count().filter(F.col("count") > 1).limit(1).count())
    if dup:
        raise RuntimeError("INEI: claves de distrito/año duplicadas")
    return norm


def guardar_parquet(df, ruta, particionar_anio=False):
    escritura = df.write.mode("errorifexists")
    if particionar_anio:
        escritura = escritura.partitionBy("anio")
    escritura.parquet(ruta)
    LOG.info("Escritura completada: %s", ruta)


def ejecutar(spark, raiz_raw, raiz_salida):
    sufijo = lambda nombre: f"{raiz_salida.rstrip('/')}/processed/{nombre}"
    src = raiz_raw.rstrip("/")

    LOG.info("Etapa 1: SINPAD histórico")
    nacional = leer_sinpad(spark, f"{src}/sinpad/BD_2003-2025_EMERGENCIAS.csv")
    verificar_total(nacional, 142139, "SINPAD nacional")
    libertad = nacional.filter(F.col("departamento") == "LA LIBERTAD")
    verificar_total(libertad, 2853, "SINPAD La Libertad")
    guardar_parquet(nacional, sufijo("emergencias_nacional"), True)
    guardar_parquet(libertad, sufijo("emergencias_la_libertad"), True)
    # La materialización en Parquet corta el linaje anterior.
    libertad = spark.read.parquet(sufijo("emergencias_la_libertad"))

    LOG.info("Etapa 2: COEN y unión LEFT con SINPAD")
    coen_raw, coen_agrupado = leer_coen(spark, f"{src}/coen/metadata_reportes_linkage.csv")
    verificar_total(coen_raw, 514, "COEN registros de metadatos")
    integrado = (libertad.join(coen_agrupado, "codigo_sinpad", "left")
                 .withColumn("tiene_reporte_coen",
                             F.when(F.col("cantidad_reportes_coen").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
                 .withColumn("cantidad_reportes_coen", F.coalesce("cantidad_reportes_coen", F.lit(0))))
    verificar_total(integrado, 2853, "SINPAD + COEN")
    LOG.info("Correspondencias COEN en La Libertad: %d",
             integrado.filter(F.col("tiene_reporte_coen") == 1).count())
    guardar_parquet(integrado, sufijo("emergencias_integradas"), True)
    integrado = spark.read.parquet(sufijo("emergencias_integradas"))

    LOG.info("Etapa 3: CHIRPS e INEI + dataset maestro")
    chirps = leer_chirps(spark, f"{src}/chirps/historico/*.csv")
    verificar_total(chirps, 23184, "CHIRPS 2003-2025")
    inei = leer_inei(spark, f"{src}/inei/inei_la_libertad_poblacion_distrital_2021_2025.csv")
    base = (integrado.withColumn("provincia_key", normalizar_clave(F.col("provincia")))
            .withColumn("distrito_key", normalizar_clave(F.col("distrito")))
            .withColumn("mes_num", F.month("fecha_emergencia")))
    lluvia = chirps.select("provincia_key", "distrito_key", "anio", "mes_num",
                           "precipitacion_total_mm", "precipitacion_promedio_diaria_mm",
                           "precipitacion_maxima_diaria_mm")
    claves_mes = ["provincia_key", "distrito_key", "anio", "mes_num"]
    maestro = (base.join(lluvia, claves_mes, "left")
               .join(inei, ["provincia_key", "distrito_key", "anio"], "left")
               .drop("provincia_key", "distrito_key"))
    verificar_total(maestro, 2853, "Dataset maestro")
    LOG.info("Dataset maestro con CHIRPS: %d", maestro.filter(F.col("precipitacion_total_mm").isNotNull()).count())
    LOG.info("Dataset maestro con INEI: %d", maestro.filter(F.col("poblacion").isNotNull()).count())
    guardar_parquet(maestro, sufijo("dataset_maestro_2003_2025"), True)
    maestro = spark.read.parquet(sufijo("dataset_maestro_2003_2025"))

    LOG.info("Etapa 4: panel distrito-mes con meses sin emergencia")
    panel_base = chirps.select("ubigeo", "provincia", "distrito", "provincia_key", "distrito_key",
                               "anio", "mes_num", "precipitacion_total_mm",
                               "precipitacion_promedio_diaria_mm", "precipitacion_maxima_diaria_mm")
    eventos_base = maestro.withColumn("mes_num", F.month("fecha_emergencia"))
    eventos = (eventos_base.withColumn("provincia_key", normalizar_clave(F.col("provincia")))
               .withColumn("distrito_key", normalizar_clave(F.col("distrito")))
               .groupBy(*claves_mes)
               .agg(F.count("*").alias("cantidad_emergencias_mes"),
                    F.countDistinct("peligro").alias("tipos_peligro_mes"),
                    *[F.sum(k).alias(f"{k}_mes") for k in (
                        "afectados", "damnificados", "fallecidos", "desaparecidos",
                        "viviendas_afectadas", "viviendas_destruidas")]))
    panel = panel_base.join(eventos, claves_mes, "left").fillna({
        "cantidad_emergencias_mes": 0, "tipos_peligro_mes": 0,
        **{f"{k}_mes": 0 for k in ("afectados", "damnificados", "fallecidos",
                                    "desaparecidos", "viviendas_afectadas", "viviendas_destruidas")}
    })
    verificar_total(panel, 23184, "Panel distrito-mes")
    LOG.info("Total emergencias del panel: %s",
             panel.agg(F.sum("cantidad_emergencias_mes")).first()[0])
    guardar_parquet(panel, sufijo("panel_distrito_mes_2003_2025"), True)
    LOG.info("PIPELINE OK: RAW -> PROCESSED. Salida: %s", raiz_salida)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-base", default="hdfs://localhost:9000/proyecto_emergencias/raw")
    parser.add_argument("--output-base", default="hdfs://localhost:9000/proyecto_emergencias/reproduccion_pipeline")
    args = parser.parse_args()
    if args.output_base.rstrip("/") in (args.raw_base.rstrip("/"),
                                         "hdfs://localhost:9000/proyecto_emergencias/processed",
                                         "hdfs://localhost:9000/proyecto_emergencias"):
        parser.error("--output-base debe ser una carpeta separada de RAW y de los datos existentes")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    spark = (SparkSession.builder.appName("ReproduccionEmergenciasBigData")
             .master("yarn").getOrCreate())
    try:
        LOG.info("Spark master: %s", spark.sparkContext.master)
        ejecutar(spark, args.raw_base, args.output_base)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
