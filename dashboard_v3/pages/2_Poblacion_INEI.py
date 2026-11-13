import glob
from pathlib import Path
import pandas as pd
import streamlit as st

st.title("👥 Población distrital - La Libertad")
st.caption("Fuente: INEI | Año de referencia: 2025")

archivos = glob.glob(
    str(Path(__file__).resolve().parents[2] / "data" / "inei" / "inei_la_libertad_poblacion_distrital_2021*.csv")
)

if not archivos:
    st.error("No se encontró el archivo de población INEI.")
    st.stop()

df = pd.read_csv(archivos[0], encoding="utf-8-sig")
df.columns = df.columns.str.strip()

df = df[df["anio"] == 2025].copy()
df["poblacion"] = pd.to_numeric(df["poblacion"], errors="coerce")
df = df.dropna(subset=["poblacion"])
df = df.sort_values("poblacion", ascending=False).reset_index(drop=True)

# Clasificación relativa por cantidad de habitantes
n = len(df)
df["Prioridad poblacional"] = [
    "Alta" if i < n / 3 else "Media" if i < 2 * n / 3 else "Baja"
    for i in range(n)
]

df["Distrito"] = df["distrito"] + " (" + df["provincia"] + ")"

c1, c2 = st.columns(2)
c1.metric("Distritos", len(df))
c2.metric("Población total", f"{int(df['poblacion'].sum()):,}")

st.subheader("Consultar distrito")
seleccion = st.selectbox("Selecciona un distrito", df["Distrito"])

fila = df[df["Distrito"] == seleccion].iloc[0]

c1, c2 = st.columns(2)
c1.metric("Habitantes", f"{int(fila['poblacion']):,}")
c2.metric("Prioridad poblacional", fila["Prioridad poblacional"])

st.subheader("Población por distrito")

tabla = df[["Distrito", "poblacion", "Prioridad poblacional"]].copy()
tabla.columns = ["Distrito", "Habitantes", "Prioridad poblacional"]

st.dataframe(tabla, hide_index=True, use_container_width=True)

st.subheader("10 distritos más poblados")
top = df.head(10).set_index("Distrito")
st.bar_chart(top["poblacion"])

st.info(
    "La prioridad poblacional compara la cantidad de habitantes "
    "entre los 84 distritos. No representa riesgo de emergencia "
    "ni modifica las predicciones del modelo V3-A."
)
