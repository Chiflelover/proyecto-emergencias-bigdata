from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F

BASE = Path(__file__).resolve().parent
ENTRADA = BASE / "predicciones_generadas"
SALIDA = BASE / "comparaciones_sinpad_2026"
SALIDA.mkdir(exist_ok=True)

spark = (
    SparkSession.builder
    .appName("Comparacion_SINPAD_2026")
    .master("local[1]")
    .config("spark.driver.memory", "1536m")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    ruta = (
        "hdfs://localhost:9000/proyecto_emergencias/"
        "processed/panel_distrito_mes_2026_provisional_v2"
    )

    sinpad = (
        spark.read.parquet(ruta)
        .select(
            F.lpad(F.col("ubigeo").cast("string"), 6, "0").alias("ubigeo"),
            "anio", "mes_num",
            "cantidad_emergencias_mes",
            "reportes_sinpad",
            "precipitacion_total_mm"
        )
        .toPandas()
    )

    historico = (
        spark.read.parquet(
            "hdfs://localhost:9000/proyecto_emergencias/"
            "processed/panel_distrito_mes_2003_2025"
        )
        .select(
            F.lpad(F.col("ubigeo").cast("string"), 6, "0").alias("ubigeo"),
            "mes_num", "precipitacion_total_mm"
        )
        .toPandas()
    )

finally:
    spark.stop()

umbrales = (
    historico.groupby(["ubigeo", "mes_num"])["precipitacion_total_mm"]
    .agg(
        p25=lambda x: x.quantile(0.25),
        p75=lambda x: x.quantile(0.75)
    )
    .reset_index()
)

archivos = sorted(ENTRADA.glob("prediccion_2026-??.csv"))

if not archivos:
    raise SystemExit("No hay predicciones 2026 disponibles.")

for archivo in archivos:
    periodo = archivo.stem.replace("prediccion_", "")
    anio, mes = map(int, periodo.split("-"))

    pred = pd.read_csv(
        archivo, encoding="utf-8-sig",
        dtype={"ubigeo": str}
    )
    pred["ubigeo"] = pred["ubigeo"].str.zfill(6)

    real = sinpad[
        (sinpad["anio"] == anio) &
        (sinpad["mes_num"] == mes)
    ].copy()

    if real.empty:
        print(f"{periodo}: SINPAD aún no disponible")
        continue

    real = real[[
        "ubigeo", "cantidad_emergencias_mes",
        "reportes_sinpad", "precipitacion_total_mm"
    ]]

    real = real.merge(
        umbrales.loc[
            umbrales["mes_num"] == mes,
            ["ubigeo", "p25", "p75"]
        ],
        on="ubigeo",
        how="left",
        validate="one_to_one"
    )

    if pred["ubigeo"].duplicated().any() or real["ubigeo"].duplicated().any():
        raise ValueError(f"{periodo}: UBIGEO duplicado")

    # Partir de todos los distritos con datos del mes,
    # no solamente de los que recibieron una predicción.
    resultado = real.merge(
        pred, on="ubigeo", how="left", validate="one_to_one"
    )

    # Recuperar nombres oficiales del catálogo de 84 distritos.
    referencia = pd.read_csv(
        ENTRADA / "prediccion_2026-01.csv",
        dtype={"ubigeo": str},
        encoding="utf-8-sig"
    )
    referencia["ubigeo"] = referencia["ubigeo"].str.zfill(6)

    if len(referencia) != 84 or referencia["ubigeo"].duplicated().any():
        raise ValueError("Catálogo de distritos inválido")

    nombres = referencia.set_index("ubigeo")

    for campo in ["provincia", "distrito"]:
        resultado[campo] = resultado["ubigeo"].map(nombres[campo])

    if resultado["distrito"].isna().any():
        raise ValueError("Hay distritos sin identificar")


    def nivel_lluvia(fila):
        lluvia = fila["precipitacion_total_mm"]
        p25 = fila["p25"]
        p75 = fila["p75"]

        if pd.isna(lluvia) or pd.isna(p25) or pd.isna(p75):
            return "Sin datos"

        if lluvia > p75:
            return "Alta"
        if lluvia < p25:
            return "Baja"
        return "Normal"

    resultado["nivel_precipitacion"] = resultado.apply(
        nivel_lluvia, axis=1
    )

    resultado["prioridad"] = resultado["puesto_modelo"].apply(
        lambda x: "Prioritario (Top 20)" if x <= 20 else "Fuera del Top 20"
    )

    def comparar(fila):
        if pd.isna(fila["reportes_sinpad"]):
            return "Sin información SINPAD"
        if fila["reportes_sinpad"] <= 0:
            return "Sin reporte disponible"
        if fila["puesto_modelo"] <= 20:
            return "Coincidencia Top 20"
        return "Emergencia publicada fuera del Top 20"

    resultado["comparacion"] = resultado.apply(comparar, axis=1)
    if len(pred) < 84:
        resultado["prioridad"] = f"Ranking parcial ({len(pred)}/84)"
        resultado["comparacion"] = resultado["reportes_sinpad"].apply(
            lambda n: (
                "Sin información SINPAD" if pd.isna(n)
                else "Emergencia publicada (ranking parcial)"
                if n > 0
                else "Sin reporte disponible"
            )
        )

    # Distinguir predicción disponible de información insuficiente.
    con_score = resultado["score_modelo_pct"].notna()
    publicado = resultado["reportes_sinpad"].fillna(0) > 0

    resultado.loc[~con_score, "prioridad"] = "Sin score V3-A"
    resultado.loc[
        ~con_score & publicado, "comparacion"
    ] = "Emergencia publicada; sin predicción"

    resultado.loc[
        ~con_score & ~publicado, "comparacion"
    ] = "Sin reporte publicado; sin predicción"

    resultado.loc[
        con_score & ~publicado, "comparacion"
    ] = "Sin reporte publicado"

    if len(pred) < 84:
        resultado.loc[
            con_score, "prioridad"
        ] = "Ranking parcial V3-A"

    # Un cero sin reporte SINPAD no se presenta como
    # ausencia confirmada de emergencias.
    resultado.loc[
        ~publicado, "cantidad_emergencias_mes"
    ] = float("nan")

    resultado = resultado.sort_values("puesto_modelo")

    destino = SALIDA / f"comparacion_{periodo}.csv"
    resultado.to_csv(destino, index=False, encoding="utf-8-sig")

    con_score = resultado["score_modelo_pct"].notna()
    publicado = resultado["reportes_sinpad"].fillna(0) > 0

    if len(pred) < 84:
        coincidencias = int((con_score & publicado).sum())
        etiqueta = "Con emergencia publicada entre evaluados"
    else:
        coincidencias = int((
            resultado["puesto_modelo"].le(20) & publicado
        ).sum())
        etiqueta = "Coincidencias Top 20"

    print(f"Con score V3-A: {int(con_score.sum())} / {len(resultado)}")
    print(f"{periodo}: {len(resultado)} distritos")
    print(f"{etiqueta}: {coincidencias}")
    print(f"Guardado: {destino}")
