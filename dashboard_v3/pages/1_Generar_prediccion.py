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
    "2025: demostración histórica. "
    "2026: predicciones exploratorias con información SINPAD provisional."
)

meses = {
    1: "Enero", 2: "Febrero", 3: "Marzo",
    4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre",
    10: "Octubre", 11: "Noviembre",
    12: "Diciembre"
}

periodos = (
    [(2025, m) for m in range(1, 13)]
    + [(2026, m) for m in range(1, 13)]
)

anio, mes = st.selectbox(
    "Mes de entrada",
    options=periodos,
    index=17,
    format_func=lambda p: f"{meses[p[1]]} {p[0]}"
)

objetivo = (
    date(anio + 1, 1, 1)
    if mes == 12
    else date(anio, mes + 1, 1)
)

if anio == 2026:
    if mes <= 6:
        st.warning(
            "PREDICCIÓN EXPLORATORIA: SINPAD 2026 tiene "
            "cobertura incompleta y datos de daños faltantes. "
            "Los acumulados pueden subestimar impactos. "
            "El resultado no es una predicción validada ni "
            "debe utilizarse para decisiones operativas."
        )
        if mes == 6:
            st.warning(
                "Junio 2026: 9 distritos con reportes SINPAD "
                "y 75 sin reportes disponibles."
            )
    else:
        st.error(
            "Este mes aún no tiene un panel SINPAD "
            "integrado suficiente para generar una predicción."
        )

st.write(f"**Mes pronosticado:** {meses[objetivo.month]} {objetivo.year}")
st.write("**Ámbito regional:** 84 distritos")
st.write("**Variables predictoras:** 19")

if st.button(
    "Generar predicción",
    type="primary",
    disabled=(anio == 2026 and mes > 6)
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
                        str(anio),
                        str(mes),
                        *(["--exploratorio"] if anio == 2026 else [])
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

    cantidad = len(df)

    if (
        cantidad == 0
        or cantidad > 84
        or df["ubigeo"].isna().any()
        or df["ubigeo"].nunique() != cantidad
        or (anio == 2025 and cantidad != 84)
    ):
        st.error("El archivo contiene una cobertura inválida.")
        st.stop()

    if anio == 2026 and cantidad < 84:
        st.warning(
            f"Predicción exploratoria: {cantidad} de 84 distritos "
            "tienen puntuación V3-A. Los demás no cuentan con "
            "información suficiente."
        )

    st.success(
        f"Predicción {periodo} generada correctamente."
    )

    if anio == 2026:
        ruta_lluvia = (
            CARPETA / "comparaciones_sinpad_2026"
            / f"comparacion_{anio}-{mes:02d}.csv"
        )

        if ruta_lluvia.exists():
            base_lluvia = pd.read_csv(
                ruta_lluvia, dtype={"ubigeo": str}
            )
            base_lluvia["ubigeo"] = (
                base_lluvia["ubigeo"].str.zfill(6)
            )

            if (
                len(base_lluvia) != 84
                or base_lluvia["ubigeo"].duplicated().any()
            ):
                st.error("El catálogo de 84 distritos es inválido.")
                st.stop()

            pred = df[[
                "ubigeo", "puesto_modelo", "score_modelo_pct"
            ]].copy()
            pred["ubigeo"] = pred["ubigeo"].str.zfill(6)

            resumen = base_lluvia[[
                "ubigeo", "provincia", "distrito",
                "precipitacion_total_mm", "nivel_precipitacion"
            ]].merge(
                pred, on="ubigeo", how="left",
                validate="one_to_one"
            )

            def clasificar(fila):
                score = fila["score_modelo_pct"]
                lluvia = fila["nivel_precipitacion"]

                if pd.notna(score):
                    return "Prioridad V3-A"

                if lluvia == "Alta":
                    return "Vigilancia por lluvia alta"

                if lluvia == "Normal":
                    return "Sin score V3-A; lluvia normal"

                if lluvia == "Baja":
                    return "Sin score V3-A; lluvia baja"

                return "Información insuficiente"

            resumen["atencion"] = resumen.apply(
                clasificar, axis=1
            )

            resumen["_con_score"] = (
                resumen["score_modelo_pct"].notna()
            )
            resumen["_lluvia_alta"] = (
                resumen["nivel_precipitacion"] == "Alta"
            )

            resumen = resumen.sort_values(
                [
                    "_con_score", "score_modelo_pct",
                    "_lluvia_alta", "provincia", "distrito"
                ],
                ascending=[False, False, False, True, True],
                na_position="last"
            )

            st.subheader("Priorización territorial — 84 distritos")

            st.caption(
                f"Precipitación observada en {meses[mes]} {anio}, "
                "antes del mes pronosticado. "
                "La clasificación Alta/Normal/Baja es relativa "
                "al historial del distrito; no constituye una "
                "predicción meteorológica ni una alerta oficial. "
                "Los distritos sin score no fueron evaluados por V3-A."
            )

            columnas = {
                "puesto_modelo": "Puesto V3-A",
                "provincia": "Provincia",
                "distrito": "Distrito",
                "score_modelo_pct": "Score V3-A",
                "precipitacion_total_mm": "Lluvia (mm)",
                "nivel_precipitacion": "Nivel de lluvia",
                "atencion": "Atención sugerida"
            }

            st.dataframe(
                resumen[list(columnas)].rename(columns=columnas),
                hide_index=True,
                use_container_width=True,
                height=650
            )

            st.info(
                f"{len(pred)} distritos con score V3-A; "
                f"{84 - len(pred)} sin score. "
                "Los 84 aparecen en la tabla."
            )

        else:
            st.warning(
                "Faltan los datos de precipitación del mes "
                "de entrada para completar la tabla de 84 distritos."
            )

    st.metric("Distritos pronosticados", len(df))

    st.subheader("Ranking de distritos con score V3-A")

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
