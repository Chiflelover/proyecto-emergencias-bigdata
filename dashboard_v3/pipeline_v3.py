
import math
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def construir_variables_v3(panel):
    """Construye las 19 variables del modelo V3-A."""

    w = Window.partitionBy("ubigeo").orderBy(
        "anio", "mes_num"
    )

    df = panel

    # Desfases temporales
    desfases = [
        ("emergencias_mes_anterior",
         "cantidad_emergencias_mes", 1),
        ("emergencias_mismo_mes_anio_anterior",
         "cantidad_emergencias_mes", 12),
        ("lluvia_mes_anterior",
         "precipitacion_total_mm", 1)
    ]

    for nueva, origen, meses in desfases:
        df = df.withColumn(
            nueva,
            F.coalesce(
                F.lag(origen, meses).over(w),
                F.lit(0)
            )
        )

    # Acumulados históricos
    acumulados = [
        ("emergencias_3_meses",
         "cantidad_emergencias_mes", 3),
        ("emergencias_6_meses",
         "cantidad_emergencias_mes", 6),
        ("emergencias_12_meses",
         "cantidad_emergencias_mes", 12),
        ("afectados_12_meses",
         "afectados_mes", 12),
        ("damnificados_12_meses",
         "damnificados_mes", 12),
        ("fallecidos_12_meses",
         "fallecidos_mes", 12),
        ("viviendas_afectadas_12_meses",
         "viviendas_afectadas_mes", 12),
        ("viviendas_destruidas_12_meses",
         "viviendas_destruidas_mes", 12),
        ("lluvia_3_meses",
         "precipitacion_total_mm", 3)
    ]

    for nueva, origen, meses in acumulados:
        df = df.withColumn(
            nueva,
            F.coalesce(
                F.sum(origen).over(
                    w.rowsBetween(-meses, -1)
                ),
                F.lit(0)
            )
        )

    # Variables estacionales
    angulo = (
        F.lit(2 * math.pi)
        * F.col("mes_num")
        / F.lit(12)
    )

    df = (
        df
        .withColumn("mes_sin", F.sin(angulo))
        .withColumn("mes_cos", F.cos(angulo))
    )

    return df


def validar_periodo_v3(panel, anio, mes):
    """Verifica la cobertura mensual necesaria para predecir."""

    if not 1 <= mes <= 12:
        raise ValueError("El mes debe estar entre 1 y 12")

    fecha_corte = F.to_date(
        F.lit(f"{anio}-{mes:02d}-01")
    )

    fecha_inicio = F.add_months(fecha_corte, -12)

    # Campos que necesita la construcción de variables
    campos_requeridos = [
        "ubigeo", "anio", "mes_num",
        "cantidad_emergencias_mes",
        "tipos_peligro_mes",
        "afectados_mes",
        "damnificados_mes",
        "fallecidos_mes",
        "viviendas_afectadas_mes",
        "viviendas_destruidas_mes",
        "precipitacion_total_mm",
        "precipitacion_promedio_diaria_mm",
        "precipitacion_maxima_diaria_mm"
    ]

    faltantes = [
        c for c in campos_requeridos
        if c not in panel.columns
    ]

    if faltantes:
        raise ValueError(
            f"Columnas faltantes: {faltantes}"
        )

    # Seleccionar los 13 meses necesarios
    datos = (
        panel
        .withColumn(
            "fecha_mes",
            F.make_date(
                F.col("anio"),
                F.col("mes_num"),
                F.lit(1)
            )
        )
        .filter(
            (F.col("fecha_mes") >= fecha_inicio) &
            (F.col("fecha_mes") <= fecha_corte)
        )
    )


    # Verificar cobertura oficial para datos de 2026 en adelante.
    # No basta con tener 84 distritos y variables numéricas.
    if anio >= 2026:

        if "sinpad_cobertura_validada" not in datos.columns:
            print(
                "BLOQUEADO: falta información sobre "
                "la validación oficial de SINPAD."
            )
            return False

        pendientes = datos.filter(
            (F.col("anio") >= 2026)
            & (
                ~F.coalesce(
                    F.col("sinpad_cobertura_validada"),
                    F.lit(False)
                )
            )
        ).count()

        if pendientes > 0:
            print("===== VALIDACIÓN SINPAD 2026 =====")
            print("Filas sin cobertura validada:", pendientes)
            print("Estado: BLOQUEADO")
            return False

    total = datos.count()

    distritos = datos.select(
        "ubigeo"
    ).distinct().count()

    # Cada distrito debe tener 13 meses
    incompletos = (
        datos
        .groupBy("ubigeo")
        .agg(
            F.countDistinct("fecha_mes").alias("meses")
        )
        .filter(F.col("meses") != 13)
        .count()
    )

    duplicados = (
        datos
        .groupBy("ubigeo", "fecha_mes")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    condicion_nulos = F.lit(False)

    for campo in campos_requeridos:
        condicion_nulos = (
            condicion_nulos | F.col(campo).isNull()
        )

    nulos = datos.filter(condicion_nulos).count()

    valido = (
        total == 1092
        and distritos == 84
        and incompletos == 0
        and duplicados == 0
        and nulos == 0
    )

    print("===== VALIDACIÓN V3-A =====")
    print("Período:", f"{anio}-{mes:02d}")
    print("Registros:", total, "/ 1092")
    print("Distritos:", distritos, "/ 84")
    print("Distritos incompletos:", incompletos)
    print("Duplicados:", duplicados)
    print("Registros con nulos:", nulos)
    print("Estado:", "VÁLIDO" if valido else "INCOMPLETO")

    return valido


from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.functions import vector_to_array


def predecir_mes_v3(panel, anio, mes):
    """Genera el ranking V3-A del mes siguiente."""

    # 1. Validar cobertura de datos
    if not validar_periodo_v3(panel, anio, mes):
        raise ValueError(
            "No se puede predecir: periodo incompleto."
        )

    # 2. Recuperar componentes entrenados
    ruta = (
        "hdfs://localhost:9000/"
        "proyecto_emergencias/models"
    )

    assembler = VectorAssembler.load(
        f"{ruta}/assembler_v3_1mes_19vars"
    )

    modelo = LogisticRegressionModel.load(
        f"{ruta}/lr_distrito_mes_v3_1mes_19vars"
    )

    # 3. Procesar solo los 13 meses necesarios
    fecha_corte = F.make_date(
        F.lit(anio), F.lit(mes), F.lit(1)
    )

    fecha_inicio = F.add_months(
        fecha_corte, -12
    )

    fecha_registro = F.make_date(
        F.col("anio"),
        F.col("mes_num"),
        F.lit(1)
    )

    panel_reciente = panel.filter(
        (fecha_registro >= fecha_inicio) &
        (fecha_registro <= fecha_corte)
    )

    # Construir las mismas 19 variables
    datos = construir_variables_v3(panel_reciente)

    datos_mes = datos.filter(
        (F.col("anio") == anio) &
        (F.col("mes_num") == mes)
    )

    # 4. Comprobar variables requeridas
    variables = assembler.getInputCols()

    faltantes = [
        c for c in variables
        if c not in datos_mes.columns
    ]

    if faltantes:
        raise ValueError(
            f"Variables faltantes: {faltantes}"
        )

    condicion_nulos = F.lit(False)

    for variable in variables:
        condicion_nulos = (
            condicion_nulos |
            F.col(variable).isNull()
        )

    if datos_mes.filter(condicion_nulos).count() > 0:
        raise ValueError(
            "Existen variables predictoras nulas."
        )

    # 5. Ejecutar el modelo V3-A
    vectorizados = assembler.transform(datos_mes)
    predicciones = modelo.transform(vectorizados)

    # 6. Generar resultados
    resultados = (
        predicciones
        .withColumn(
            "score_modelo_pct",
            F.round(
                vector_to_array("probability")[1] * 100,
                2
            )
        )
        .withColumn(
            "fecha_objetivo",
            F.add_months(
                F.make_date(
                    F.col("anio"),
                    F.col("mes_num"),
                    F.lit(1)
                ),
                1
            )
        )
        .select(
            F.lpad(
                F.col("ubigeo").cast("string"),
                6, "0"
            ).alias("ubigeo"),
            "provincia",
            "distrito",
            "fecha_objetivo",
            "score_modelo_pct"
        )
    )

    # 7. Ordenar los 84 distritos
    ventana = Window.orderBy(
        F.desc("score_modelo_pct"),
        F.asc("ubigeo")
    )

    ranking = resultados.withColumn(
        "puesto_modelo",
        F.row_number().over(ventana)
    )

    if ranking.count() != 84:
        raise ValueError(
            "La prediccion no contiene 84 distritos."
        )

    return ranking.orderBy("puesto_modelo")
