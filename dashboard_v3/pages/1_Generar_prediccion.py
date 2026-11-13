from pathlib import Path
from datetime import date
import subprocess
import sys
import fcntl

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Generar predicción V3-A",
    page_icon="📅",
    layout="wide"
)

CARPETA = Path(__file__).resolve().parent.parent
MOTOR = CARPETA / "generar_prediccion.py"

st.title("Generar predicción mensual")
st.caption("Modelo V3-A | Región La Libertad")

st.info(
    "Modo de demostración histórica. "
    "Por ahora se admiten meses de entrada de 2025, "
    "cuya información ya fue validada."
)

meses = {
    1: "Enero", 2: "Febrero", 3: "Marzo",
    4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre",
    10: "Octubre", 11: "Noviembre",
    12: "Diciembre"
}

mes = st.selectbox(
    "Mes de entrada",
    options=list(meses),
    index=11,
    format_func=lambda m: f"{meses[m]} 2025"
)

objetivo = date(2026, 1, 1) if mes == 12 else date(2025, mes + 1, 1)

st.write(f"**Mes pronosticado:** {meses[objetivo.month]} {objetivo.year}")
st.write("**Cobertura:** 84 distritos")
st.write("**Variables predictoras:** 19")

if st.button(
    "Generar predicción",
    type="primary"
):
    if not MOTOR.exists():
        st.error("No se encontró generar_prediccion.py")
        st.stop()

    # Bloquear ejecuciones simultáneas desde esta página.
    ruta_bloqueo = CARPETA / ".prediccion_v3.lock"

    with open(ruta_bloqueo, "w") as bloqueo:
        try:
            fcntl.flock(
                bloqueo,
                fcntl.LOCK_EX | fcntl.LOCK_NB
            )
        except BlockingIOError:
            st.warning(
                "Ya hay una predicción en ejecución."
            )
            st.stop()

        with st.spinner("Ejecutando el modelo V3-A con Spark..."):
            try:
                proceso = subprocess.run(
                    [
                        sys.executable,
                        str(MOTOR),
                        "2025",
                        str(mes)
                    ],
                    cwd=str(CARPETA),
                    capture_output=True,
                    text=True,
                    timeout=300
                )
            except subprocess.TimeoutExpired:
                st.error(
                    "La ejecución superó el límite configurado."
                )
                st.stop()

        if proceso.returncode != 0:
            st.error("No se pudo generar la predicción.")
            st.code(
                proceso.stderr[-4000:],
                language="text"
            )
            st.stop()

    periodo = objetivo.strftime("%Y-%m")

    archivo = (
        CARPETA
        / "predicciones_generadas"
        / f"prediccion_{periodo}.csv"
    )

    if not archivo.exists():
        st.error("El motor no generó el CSV esperado.")
        st.stop()

    df = pd.read_csv(
        archivo,
        dtype={"ubigeo": str}
    )

    if len(df) != 84 or df["ubigeo"].nunique() != 84:
        st.error("El archivo no contiene 84 distritos únicos.")
        st.stop()

    st.success(
        f"Predicción {periodo} generada correctamente."
    )

    st.metric("Distritos pronosticados", len(df))

    st.subheader("Top 20 de prioridad")

    st.dataframe(
        df.sort_values("puesto_modelo")[
            [
                "puesto_modelo",
                "provincia",
                "distrito",
                "score_modelo_pct"
            ]
        ].head(20),
        hide_index=True,
        width="stretch"
    )

    st.caption(
        "El score indica prioridad relativa y no "
        "una probabilidad calibrada de emergencia."
    )
