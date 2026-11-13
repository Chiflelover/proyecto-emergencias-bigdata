
from pathlib import Path
import json

import pandas as pd
import plotly.express as px
import streamlit as st


# ============================================
# 1. CONFIGURACIÓN
# ============================================

st.set_page_config(
    page_title="Sistema Predictivo de Emergencias",
    page_icon="📊",
    layout="wide"
)

CARPETA = Path(__file__).parent

# ============================================
# SELECTOR DINÁMICO DE PREDICCIONES
# ============================================

MESES_ES = [
    "Enero", "Febrero", "Marzo", "Abril",
    "Mayo", "Junio", "Julio", "Agosto",
    "Septiembre", "Octubre", "Noviembre",
    "Diciembre"
]

# Períodos con evaluación histórica disponible
archivos_por_periodo = {
    "Octubre 2025": "prediccion_octubre_2025.csv",
    "Noviembre 2025": "prediccion_noviembre_2025.csv",
    "Diciembre 2025": "prediccion_diciembre_2025.csv"
}

# Buscar automáticamente nuevas predicciones
carpeta_predicciones = CARPETA / "predicciones_generadas"

if carpeta_predicciones.exists():
    for ruta in sorted(
        carpeta_predicciones.glob("prediccion_????-??.csv")
    ):
        periodo = ruta.stem.replace("prediccion_", "")

        try:
            anio, mes = map(int, periodo.split("-"))

            if not 1 <= mes <= 12:
                continue

            etiqueta = f"{MESES_ES[mes - 1]} {anio}"

            archivos_por_periodo.setdefault(
                etiqueta,
                str(ruta.relative_to(CARPETA))
            )
        except ValueError:
            continue

opciones = list(archivos_por_periodo)

periodo_seleccionado = st.sidebar.selectbox(
    "Mes pronosticado",
    opciones,
    index=opciones.index("Noviembre 2025")
)

ARCHIVO_CSV = (
    CARPETA / archivos_por_periodo[periodo_seleccionado]
)

ARCHIVO_GEOJSON = (
    CARPETA / "distritos_la_libertad_plotly.geojson"
)


# ============================================
# 2. CARGA DE DATOS
# ============================================

@st.cache_data
def cargar_datos(ruta_csv):
    df = pd.read_csv(
        ruta_csv,
        dtype={"ubigeo": str}
    )

    df["ubigeo"] = df["ubigeo"].str.zfill(6)

    return df


@st.cache_data
def cargar_geojson():
    with open(
        ARCHIVO_GEOJSON,
        "r",
        encoding="utf-8"
    ) as archivo:
        geojson = json.load(archivo)

    return geojson


try:
    df = cargar_datos(ARCHIVO_CSV)
    geojson = cargar_geojson()

    tiene_evaluacion = {
        "ocurrio_realmente",
        "puesto_historico",
        "emergencias_12_meses"
    }.issubset(df.columns)

    # Columnas opcionales: no inventar resultados reales
    for columna in [
        "ocurrio_realmente",
        "puesto_historico",
        "emergencias_12_meses"
    ]:
        if columna not in df.columns:
            df[columna] = float("nan")

except Exception as error:
    st.error(f"Error al cargar los archivos: {error}")
    st.stop()


# ============================================
# 3. ENCABEZADO
# ============================================

st.title("Sistema Predictivo de Emergencias")

tab_prediccion, tab_calidad = st.tabs(["Predicción y análisis", "Calidad de datos 2026"])

with tab_prediccion:
    st.subheader(
        "Región La Libertad | Modelo V3-A"
    )

    st.caption(
        "Proyecto Big Data y Analítica de Datos - UPAO"
    )

    st.info(
        f"Mes pronosticado: {periodo_seleccionado}. "
        f"Datos utilizados hasta {df['periodo_entrada'].iloc[0]}. "
        + (
            "Evaluación retrospectiva con resultados observados."
            if tiene_evaluacion
            else "Predicción sin evaluación incorporada."
        )
    )

    st.divider()


    # ============================================
    # 4. INDICADORES DEL MODELO
    # ============================================


    if tiene_evaluacion:
        reales = df["ocurrio_realmente"] == 1
        total_reales = int(reales.sum())

        aciertos_v3 = int(
            ((df["puesto_modelo"] <= 20) & reales).sum()
        )
        aciertos_historico = int(
            ((df["puesto_historico"] <= 20) & reales).sum()
        )

        recall_v3 = (
            aciertos_v3 / total_reales * 100
            if total_reales else 0
        )
        recall_historico = (
            aciertos_historico / total_reales * 100
            if total_reales else 0
        )
        precision_v3 = aciertos_v3 / 20 * 100
        mejora = recall_v3 - recall_historico

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Distritos evaluados", len(df))
    c2.metric(
        "Recall@20 V3-A",
        f"{recall_v3:.2f}%" if tiene_evaluacion else "Sin evaluar"
    )
    c3.metric(
        "Precision@20",
        f"{precision_v3:.2f}%" if tiene_evaluacion else "Sin evaluar"
    )
    c4.metric(
        "Mejora frente al histórico",
        f"{mejora:+.2f} pp" if tiene_evaluacion else "Sin evaluar"
    )

    st.divider()


    # ============================================
    # 5. FILTROS INTERACTIVOS
    # ============================================

    st.subheader("Filtros de análisis")

    col_filtro1, col_filtro2 = st.columns(2)

    with col_filtro1:
        provincias = (
            ["Todas"]
            + sorted(df["provincia"].unique().tolist())
        )

        provincia_seleccionada = st.selectbox(
            "Seleccionar provincia",
            provincias
        )

    with col_filtro2:
        mostrar_top20 = st.checkbox(
            "Mostrar únicamente el Top 20 en el ranking",
            value=True
        )


    # ============================================
    # 6. RANKING PREDICTIVO
    # ============================================

    st.subheader("Ranking de prioridad territorial")

    datos_ranking = df.copy()

    if provincia_seleccionada != "Todas":
        datos_ranking = datos_ranking[
            datos_ranking["provincia"]
            == provincia_seleccionada
        ]

    if mostrar_top20:
        datos_ranking = datos_ranking[
            datos_ranking["puesto_modelo"] <= 20
        ]

    datos_ranking = datos_ranking.sort_values(
        "puesto_modelo"
    )

    st.dataframe(
        datos_ranking[
            ["puesto_modelo", "provincia", "distrito", "score_modelo_pct"]
            + (
                ["emergencias_12_meses", "ocurrio_realmente"]
                if tiene_evaluacion else []
            )
        ],
        hide_index=True,
        width="stretch",
        column_config={
            "puesto_modelo": "Puesto",
            "provincia": "Provincia",
            "distrito": "Distrito",
            "score_modelo_pct": st.column_config.NumberColumn(
                "Score V3-A (%)",
                format="%.2f"
            ),
            "emergencias_12_meses": "Emergencias históricas",
            "ocurrio_realmente": "Emergencia real (0/1)"
        }
    )

    st.caption(
        "El score del modelo representa prioridad relativa, "
        "no una probabilidad calibrada de emergencia."
    )

    st.divider()


    # ============================================
    # 7. MAPA INTERACTIVO
    # ============================================

    st.subheader("Mapa de prioridad territorial")

    st.write(
        "Distribución geográfica de los distritos "
        "de La Libertad según el score del modelo V3-A."
    )

    # El filtro por provincia se aplica al mapa.
    # El Top 20 solo afecta a la tabla anterior.
    datos_mapa = df.copy()

    if provincia_seleccionada != "Todas":
        datos_mapa = datos_mapa[
            datos_mapa["provincia"]
            == provincia_seleccionada
        ]

    # Crear mapa con MapLibre
    fig_mapa = px.choropleth_map(
        datos_mapa,
        geojson=geojson,
        locations="ubigeo",
        featureidkey="properties.UBIGEO",
        color="score_modelo_pct",
        hover_name="distrito",
        hover_data={
            "provincia": True,
            "ubigeo": True,
            "puesto_modelo": True,
            "score_modelo_pct": ":.2f"
        },
        color_continuous_scale="YlOrRd",
        range_color=(0, 100),
        map_style="open-street-map",
        center={
            "lat": -7.95,
            "lon": -78.30
        },
        zoom=7.7,
        opacity=0.75,
        labels={
            "provincia": "Provincia",
            "ubigeo": "UBIGEO",
            "puesto_modelo": "Puesto",
            "score_modelo_pct": "Score V3-A (%)"
        }
    )

    # Resaltar los límites distritales
    fig_mapa.update_traces(
        marker_line_width=0.7,
        marker_line_color="#303030"
    )

    # Diseño del mapa
    fig_mapa.update_layout(
        height=680,
        margin=dict(
            l=0,
            r=0,
            t=0,
            b=0
        ),
        coloraxis_colorbar=dict(
            title="Score (%)"
        )
    )

    st.plotly_chart(
        fig_mapa,
        width="stretch",
        config={
            "scrollZoom": True
        }
    )

    st.caption(
        "Los colores indican el score de prioridad "
        "del modelo V3-A. Pasa el cursor sobre cada "
        "distrito para consultar sus resultados."
    )

    st.divider()


    # ============================================
    # 8. COMPARACIÓN DE MÉTODOS
    # ============================================

    st.subheader("Comparación de modelos")


    if tiene_evaluacion:
        comparacion = pd.DataFrame({
            "Método": ["Modelo predictivo V3-A", "Método histórico"],
            "Recall@20 (%)": [recall_v3, recall_historico]
        })

        fig_comparacion = px.bar(
            comparacion,
            x="Método",
            y="Recall@20 (%)",
            color="Método",
            text="Recall@20 (%)",
            range_y=[0, 100]
        )

        fig_comparacion.update_traces(
            texttemplate="%{text:.2f}%",
            textposition="outside"
        )
        fig_comparacion.update_layout(
            showlegend=False,
            height=400
        )

        st.plotly_chart(fig_comparacion, width="stretch")

        st.success(
            f"V3-A identificó {aciertos_v3} de {total_reales} "
            f"distritos con emergencias, frente a "
            f"{aciertos_historico} del método histórico."
        )
    else:
        st.info(
            "Este período contiene puntuaciones y ranking, "
            "pero no resultados reales incorporados. "
            "Por eso no se calculan métricas de acierto."
        )

    st.divider()

with tab_calidad:
    # ============================================
    # CALIDAD Y COBERTURA DE DATOS 2026
    # ============================================

    st.divider()
    st.subheader("Calidad y cobertura de datos 2026")

    st.caption(
        "Resumen descriptivo de SINPAD 2026, enero-junio. "
        "Información provisional; no representa "
        "predicciones del modelo."
    )

    ruta_calidad = CARPETA / "calidad_sinpad_2026.csv"

    if ruta_calidad.exists():

        calidad = pd.read_csv(ruta_calidad)

        columnas_necesarias = {
            "mes_num",
            "distritos",
            "emergencias_registradas",
            "distritos_con_reportes",
            "distritos_sin_reportes",
            "distritos_danios_incompletos",
            "distritos_sin_validacion"
        }

        if not columnas_necesarias.issubset(calidad.columns):
            st.error(
                "El archivo de calidad no contiene "
                "todas las columnas necesarias."
            )
        else:
            calidad = calidad.sort_values("mes_num").copy()

            meses = {
                1: "Enero",
                2: "Febrero",
                3: "Marzo",
                4: "Abril",
                5: "Mayo",
                6: "Junio"
            }

            calidad["Mes"] = calidad["mes_num"].map(meses)

            total_emergencias = int(
                calidad["emergencias_registradas"].sum()
            )

            total_sin_reportes = int(
                calidad["distritos_sin_reportes"].sum()
            )

            total_danios_incompletos = int(
                calidad["distritos_danios_incompletos"].sum()
            )

            total_sin_validacion = int(
                calidad["distritos_sin_validacion"].sum()
            )

            c1, c2, c3, c4 = st.columns(4)

            c1.metric(
                "Emergencias registradas",
                total_emergencias
            )

            c2.metric(
                "Distrito-mes sin reportes",
                total_sin_reportes
            )

            c3.metric(
                "Distrito-mes con daños incompletos",
                total_danios_incompletos
            )

            c4.metric(
                "Distrito-mes sin validar",
                total_sin_validacion
            )

            st.warning(
                "SINPAD 2026 mantiene cobertura provisional. "
                "La ausencia de reportes no demuestra que "
                "un distrito tuvo cero emergencias. "
                "Los datos de 2026 aún no están habilitados "
                "para generar nuevas predicciones."
            )


            st.markdown("**Cobertura mensual de reportes**")

            import altair as alt

            orden_meses = [
                "Enero", "Febrero", "Marzo",
                "Abril", "Mayo", "Junio"
            ]

            grafico_mensual = calidad[
                [
                    "Mes",
                    "distritos_con_reportes",
                    "distritos_sin_reportes"
                ]
            ].melt(
                id_vars="Mes",
                var_name="Estado",
                value_name="Distritos"
            )

            grafico_mensual["Estado"] = (
                grafico_mensual["Estado"].replace({
                    "distritos_con_reportes": "Con reportes",
                    "distritos_sin_reportes": "Sin reportes"
                })
            )

            grafico = (
                alt.Chart(grafico_mensual)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "Mes:N",
                        sort=orden_meses,
                        title="Mes de 2026",
                        axis=alt.Axis(labelAngle=0)
                    ),
                    y=alt.Y(
                        "Distritos:Q",
                        stack="zero",
                        title="Cantidad de distritos"
                    ),
                    color=alt.Color(
                        "Estado:N",
                        title="Estado"
                    ),
                    tooltip=[
                        "Mes:N",
                        "Estado:N",
                        "Distritos:Q"
                    ]
                )
                .properties(height=300)
            )

            st.altair_chart(
                grafico,
                use_container_width=True
            )

            st.markdown("**Detalle mensual de calidad**")

            tabla = calidad[
                [
                    "Mes",
                    "emergencias_registradas",
                    "distritos_con_reportes",
                    "distritos_sin_reportes",
                    "distritos_danios_incompletos"
                ]
            ].rename(columns={
                "emergencias_registradas": "Emergencias",
                "distritos_con_reportes": "Con reportes",
                "distritos_sin_reportes": "Sin reportes",
                "distritos_danios_incompletos": "Daños incompletos"
            })

            st.dataframe(
                tabla,
                hide_index=True,
                width="stretch"
            )

    else:
        st.info(
            "Todavía no está disponible el resumen "
            "de calidad de SINPAD 2026."
        )

st.divider()

st.subheader("Información del sistema")

st.write(
    "Este dashboard presenta una evaluación "
    "retrospectiva de un modelo de Machine Learning "
    "entrenado con Apache Spark MLlib."
)

st.write(
    "**Fuentes de información:** SINPAD, CHIRPS "
    "y otras fuentes institucionales integradas "
    "en el proyecto Big Data."
)

st.write(
    "**Cobertura territorial:** 84 distritos "
    "de la región La Libertad."
)

st.write(
    "**Modelo:** Regresión logística ponderada V3-A."
)

st.write(
    "**Horizonte:** Predicción de ocurrencia "
    "de al menos una emergencia en el mes siguiente."
)

st.warning(
    "Este sistema es un prototipo académico. "
    "Sus resultados no sustituyen las evaluaciones "
    "técnicas de INDECI ni constituyen alertas oficiales."
)
