from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Comparación SINPAD",
    layout="wide"
)

st.title("Comparación V3-A vs. SINPAD")
st.caption("Priorización territorial y registros publicados de 2026")

carpeta = (
    Path(__file__).resolve().parent.parent
    / "comparaciones_sinpad_2026"
)

archivos = sorted(carpeta.glob("comparacion_2026-??.csv"))

if not archivos:
    st.info("Aún no hay comparaciones disponibles.")
    st.stop()

archivo = st.selectbox(
    "Mes evaluado",
    archivos,
    format_func=lambda x: x.stem.replace("comparacion_", "")
)

df = pd.read_csv(archivo, dtype={"ubigeo": str})

if archivo.stem == "comparacion_2026-07":
    st.info(
        "Julio 2026: predicción exploratoria disponible. "
        "SINPAD julio pendiente. La precipitación mostrada "
        "corresponde a JUNIO de 2026, mes de entrada."
    )


con_score = df["score_modelo_pct"].notna()
publicados = df["reportes_sinpad"].fillna(0) > 0
cobertura_completa = int(con_score.sum()) == 84

if cobertura_completa:
    coincidencias = int(
        (df["puesto_modelo"].le(20) & publicados).sum()
    )
else:
    coincidencias = int((con_score & publicados).sum())

c1, c2, c3, c4 = st.columns(4)

c1.metric("Distritos en tabla", len(df))
c2.metric("Con score V3-A", int(con_score.sum()))
if archivo.stem == "comparacion_2026-07":
    c3.metric("SINPAD julio", "Pendiente")
    c4.metric("Comparación", "Pendiente")
else:
    c3.metric("Con emergencia publicada", int(publicados.sum()))
    c4.metric(
        "Coincidencias Top 20"
        if cobertura_completa
        else "Con emergencia entre evaluados",
        coincidencias
    )

st.warning(
    "SINPAD 2026 es provisional. Sin reporte publicado "
    "no significa ausencia confirmada de emergencias. "
    "Los distritos sin score no fueron evaluados por V3-A. "
    "La precipitación es información complementaria."
)

solo_modelo = st.checkbox(
    "Mostrar únicamente distritos con score V3-A",
    value=False
)

vista = df[con_score].copy() if solo_modelo else df.copy()
vista = vista.sort_values("puesto_modelo", na_position="last")

st.subheader("Tabla de prioridad territorial")

st.caption(
    "Ordenada por el ranking original de V3-A cuando está "
    "disponible. Lluvia alta, normal o baja según el historial "
    "del mismo mes de cada distrito (2003–2025). "
    "La lluvia no altera la puntuación V3-A."
)

columnas = {
    "puesto_modelo": "Puesto",
    "provincia": "Provincia",
    "distrito": "Distrito",
    "score_modelo_pct": "Score V3-A",
    "prioridad": "Prioridad",
    "cantidad_emergencias_mes": "Emergencias publicadas",
    "precipitacion_total_mm": "Lluvia (mm)",
    "nivel_precipitacion": "Nivel de lluvia",
    "comparacion": "Resultado SINPAD"
}

st.dataframe(
    vista[list(columnas)].rename(columns=columnas),
    hide_index=True,
    use_container_width=True,
    height=780
)

st.download_button(
    "Exportar comparación completa",
    data=archivo.read_bytes(),
    file_name=archivo.name,
    mime="text/csv"
)
