import os
import json
import argparse
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


MESES_CORTOS = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr",
    5: "May", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
}

MESES_2026 = {
    "ENERO": 1,
    "FEBRERO": 2,
    "MARZO": 3,
    "ABRIL": 4,
    "MAYO": 5,
    "JUNIO": 6,
    "JULIO": 7,
    "AGOSTO": 8,
    "SETIEMBRE": 9,
    "SEPTIEMBRE": 9,
    "OCTUBRE": 10,
    "NOVIEMBRE": 11,
    "DICIEMBRE": 12
}


def leer_geojson_2026(spark, ruta_hdfs):
    archivos = spark.sparkContext.wholeTextFiles(ruta_hdfs).collect()
    if not archivos:
        raise RuntimeError(f"No se pudo leer el GeoJSON 2026 desde: {ruta_hdfs}")

    _, contenido = archivos[0]
    geo = json.loads(contenido)

    registros = []
    for feat in geo.get("features", []):
        props = feat.get("properties", {})
        mes_txt = str(props.get("MES", "")).strip().upper()
        mes_num = MESES_2026.get(mes_txt)

        ide = props.get("IDE_SINPAD")
        if mes_num is not None and ide is not None:
            registros.append((int(ide), int(mes_num)))

    df = spark.createDataFrame(registros, ["ide_sinpad", "mes_num"]).dropDuplicates(["ide_sinpad"])
    return df


def grafico_01(df_panel, output_dir):
    df = (
        df_panel
        .groupBy("anio")
        .agg(F.sum("cantidad_emergencias_mes").alias("emergencias"))
        .orderBy("anio")
    )

    rows = df.collect()
    anios = [int(r["anio"]) for r in rows]
    emergencias = [int(r["emergencias"]) for r in rows]

    plt.figure(figsize=(14, 6))
    plt.plot(anios, emergencias, marker="o", linewidth=2)

    plt.title(
        "Evolución anual de emergencias registradas en La Libertad (2003–2025)",
        fontsize=14,
        fontweight="bold"
    )
    plt.xlabel("Año")
    plt.ylabel("Número de emergencias")
    plt.xticks(anios, rotation=45)
    plt.grid(axis="y", linestyle="--", alpha=0.4)

    for x, y in zip(anios, emergencias):
        plt.text(x, y + 10, str(y), ha="center", fontsize=8)

    plt.tight_layout()
    ruta = os.path.join(output_dir, "grafico_01_emergencias_por_anio.png")
    plt.savefig(ruta, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"OK -> {ruta}")


def grafico_02(df_emergencias, output_dir):
    df = (
        df_emergencias
        .withColumn("peligro_limpio", F.upper(F.trim(F.col("peligro"))))
        .groupBy("peligro_limpio")
        .count()
        .orderBy(F.desc("count"))
        .limit(10)
    )

    rows = df.collect()
    peligros = [r["peligro_limpio"] for r in rows]
    cantidades = [int(r["count"]) for r in rows]

    peligros_rev = peligros[::-1]
    cantidades_rev = cantidades[::-1]

    plt.figure(figsize=(11, 6))
    plt.barh(peligros_rev, cantidades_rev)

    plt.title(
        "Top 10 tipos de peligro registrados en La Libertad (2003–2025)",
        fontsize=14,
        fontweight="bold"
    )
    plt.xlabel("Número de emergencias")
    plt.ylabel("Tipo de peligro")
    plt.grid(axis="x", linestyle="--", alpha=0.4)

    for i, valor in enumerate(cantidades_rev):
        plt.text(valor + 10, i, str(valor), va="center", fontsize=9)

    plt.tight_layout()
    ruta = os.path.join(output_dir, "grafico_02_top_peligros.png")
    plt.savefig(ruta, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"OK -> {ruta}")


def grafico_03(df_panel, output_dir):
    df = (
        df_panel
        .groupBy("mes_num")
        .agg(F.sum("cantidad_emergencias_mes").alias("emergencias"))
        .orderBy("mes_num")
    )

    rows = df.collect()
    meses = [MESES_CORTOS[int(r["mes_num"])] for r in rows]
    emergencias = [int(r["emergencias"]) for r in rows]

    plt.figure(figsize=(11, 6))
    plt.bar(meses, emergencias)

    plt.title(
        "Distribución mensual de emergencias registradas en La Libertad (2003–2025)",
        fontsize=14,
        fontweight="bold"
    )
    plt.xlabel("Mes")
    plt.ylabel("Número de emergencias")
    plt.grid(axis="y", linestyle="--", alpha=0.4)

    for i, valor in enumerate(emergencias):
        plt.text(i, valor + 10, str(valor), ha="center", fontsize=9)

    plt.tight_layout()
    ruta = os.path.join(output_dir, "grafico_03_emergencias_por_mes.png")
    plt.savefig(ruta, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"OK -> {ruta}")


def grafico_04(df_panel, df_2026, output_dir):
    historico = (
        df_panel
        .filter(F.col("mes_num").between(1, 5))
        .groupBy("mes_num")
        .agg(F.sum("cantidad_emergencias_mes").alias("total_historico"))
        .orderBy("mes_num")
    )

    rows_h = historico.collect()
    meses_num = [int(r["mes_num"]) for r in rows_h]
    meses = [MESES_CORTOS[m] for m in meses_num]
    promedio_historico = [float(r["total_historico"]) / 23.0 for r in rows_h]

    observado_2026 = (
        df_2026
        .filter(F.col("mes_num").between(1, 5))
        .groupBy("mes_num")
        .count()
        .orderBy("mes_num")
    )

    rows_2026 = {int(r["mes_num"]): int(r["count"]) for r in observado_2026.collect()}
    emergencias_2026 = [rows_2026.get(m, 0) for m in meses_num]

    x = np.arange(len(meses))
    ancho = 0.36

    plt.figure(figsize=(10, 6))
    plt.bar(
        x - ancho/2,
        promedio_historico,
        ancho,
        label="Promedio histórico 2003–2025"
    )
    plt.bar(
        x + ancho/2,
        emergencias_2026,
        ancho,
        label="2026 observado"
    )

    plt.title(
        "Emergencias observadas en 2026 frente al promedio histórico",
        fontsize=14,
        fontweight="bold"
    )
    plt.xlabel("Mes")
    plt.ylabel("Número de emergencias")
    plt.xticks(x, meses)
    plt.grid(axis="y", linestyle="--", alpha=0.4)
    plt.legend()

    for i, valor in enumerate(promedio_historico):
        plt.text(i - ancho/2, valor + 4, f"{valor:.1f}", ha="center", fontsize=9)

    for i, valor in enumerate(emergencias_2026):
        plt.text(i + ancho/2, valor + 4, str(valor), ha="center", fontsize=9)

    plt.tight_layout()
    ruta = os.path.join(output_dir, "grafico_04_2026_vs_historico.png")
    plt.savefig(ruta, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"OK -> {ruta}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processed-base",
        required=True,
        help="Ruta base HDFS de processed"
    )
    parser.add_argument(
        "--sinpad2026-path",
        required=True,
        help="Ruta HDFS del GeoJSON 2026"
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Carpeta local donde se guardarán los PNG"
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("EDA_Emergencias_La_Libertad")
        .getOrCreate()
    )

    processed_base = args.processed_base.rstrip("/")

    df_panel = spark.read.parquet(f"{processed_base}/panel_distrito_mes_2003_2025")
    df_emergencias = spark.read.parquet(f"{processed_base}/emergencias_la_libertad")
    df_2026 = leer_geojson_2026(spark, args.sinpad2026_path)

    print("Generando gráfico 1...")
    grafico_01(df_panel, args.output_dir)

    print("Generando gráfico 2...")
    grafico_02(df_emergencias, args.output_dir)

    print("Generando gráfico 3...")
    grafico_03(df_panel, args.output_dir)

    print("Generando gráfico 4...")
    grafico_04(df_panel, df_2026, args.output_dir)

    spark.stop()
    print("EDA completado.")


if __name__ == "__main__":
    main()
