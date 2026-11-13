
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline_v3 import predecir_mes_v3


def main():
    parser = argparse.ArgumentParser(
        description="Predicción mensual V3-A"
    )

    parser.add_argument("anio", type=int)
    parser.add_argument("mes", type=int)
    parser.add_argument("--exploratorio", action="store_true")

    args = parser.parse_args()

    anio = args.anio
    mes = args.mes

    # Por ahora solo admitimos el panel histórico
    # completo de 2003 a 2025.
    if not (1 <= mes <= 12):
        parser.error("El mes debe estar entre 1 y 12")

    if not ((2004, 1) <= (anio, mes) <= (2026, 6)):
        parser.error("Período fuera del rango disponible.")

    if anio == 2026 and not args.exploratorio:
        parser.error(
            "Para utilizar SINPAD provisional 2026 "
            "debes indicar --exploratorio."
        )

    spark = (
        SparkSession.builder
        .appName("Prediccion_Mensual_V3A")
        .master("local[1]")
        .config("spark.driver.memory", "1536m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        ruta_panel = (
            "hdfs://localhost:9000/proyecto_emergencias/processed/"
            + (
                "panel_distrito_mes_2003_2026_provisional_v2"
                if anio == 2026
                else "panel_distrito_mes_2003_2025"
            )
        )

        print("===== MOTOR PREDICTIVO V3-A =====")
        print(f"Mes de entrada: {anio}-{mes:02d}")

        panel = spark.read.parquet(ruta_panel)

        # Ejecutar nuestro pipeline validado
        ranking = predecir_mes_v3(
            panel, anio, mes,
            modo_exploratorio=args.exploratorio
        )

        ranking = ranking.withColumn(
            "estado_prediccion",
            F.lit(
                "EXPLORATORIA_SINPAD_NO_VALIDADO"
                if anio == 2026
                else "HISTORICA"
            )
        )

        # Incorporar mes de entrada
        ranking = ranking.withColumn(
            "periodo_entrada",
            F.lit(f"{anio}-{mes:02d}")
        ).withColumn(
            "periodo",
            F.date_format(
                "fecha_objetivo", "yyyy-MM"
            )
        )

        # Exportar resultados
        carpeta = (
            Path(__file__).parent
            / "predicciones_generadas"
        )

        carpeta.mkdir(
            parents=True, exist_ok=True
        )

        datos = ranking.toPandas()
        datos["ubigeo"] = datos["ubigeo"].astype(str).str.zfill(6)

        periodo = datos["periodo"].iloc[0]

        salida = carpeta / f"prediccion_{periodo}.csv"

        datos.to_csv(
            salida,
            index=False,
            encoding="utf-8-sig"
        )

        print("\n===== RESULTADO =====")
        print("Mes pronosticado:", periodo)
        print("Distritos:", len(datos))
        print("Archivo:", salida)

        assert (
            0 < len(datos) <= 84
            if args.exploratorio
            else len(datos) == 84
        )
        assert datos["ubigeo"].nunique() == len(datos)

        print("\nPREDICCIÓN GENERADA CORRECTAMENTE")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
