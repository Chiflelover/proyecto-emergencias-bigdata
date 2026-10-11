#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

RAW_BASE="hdfs://localhost:9000/proyecto_emergencias/raw"
FECHA=$(date +%Y%m%d_%H%M%S)
OUTPUT_BASE="hdfs://localhost:9000/proyecto_emergencias/ejecucion_${FECHA}"

mkdir -p "$HOME/pipeline_logs"
LOG="$HOME/pipeline_logs/ejecucion_${FECHA}.log"

echo "======================================"
echo " PROYECTO BIG DATA - LA LIBERTAD"
echo "======================================"

# Verificar herramientas
command -v hdfs >/dev/null || { echo "ERROR: HDFS no disponible"; exit 1; }
command -v spark-submit >/dev/null || { echo "ERROR: Spark no disponible"; exit 1; }

# Verificar datos originales
hdfs dfs -test -d "$RAW_BASE" || {
    echo "ERROR: No se encontraron los datos RAW en HDFS"
    exit 1
}

echo "[1/3] Ejecutando procesamiento Spark..."

spark-submit \
    --master yarn \
    --deploy-mode client \
    src/pipeline_emergencias.py \
    --raw-base "$RAW_BASE" \
    --output-base "$OUTPUT_BASE" \
    2>&1 | tee "$LOG"

echo "Procesamiento RAW -> PROCESSED completado."
echo "Resultados: $OUTPUT_BASE"
echo "Log: $LOG"
echo "[2/3] Entrenando modelo base..."

spark-submit \
    --master yarn \
    --deploy-mode client \
    src/modelo_base.py \
    --panel-path "$OUTPUT_BASE/processed/panel_distrito_mes_2003_2025" \
    2>&1 | tee -a "$LOG"

echo "Modelo base completado."
echo "[3/3] Generando graficos del EDA..."

RESULTADOS_DIR="resultados/ejecucion_${FECHA}"
mkdir -p "$RESULTADOS_DIR"

SINPAD2026_PATH="$RAW_BASE/sinpad/2026/sinpad_la_libertad_2026.geojson"

hdfs dfs -test -e "$SINPAD2026_PATH" || {
    echo "ERROR: No se encontro el GeoJSON de 2026"
    exit 1
}

spark-submit \
    --master yarn \
    --deploy-mode client \
    src/eda_emergencias.py \
    --processed-base "$OUTPUT_BASE/processed" \
    --sinpad2026-path "$SINPAD2026_PATH" \
    --output-dir "$RESULTADOS_DIR" \
    2>&1 | tee -a "$LOG"

echo "======================================"
echo " PIPELINE COMPLETADO CORRECTAMENTE"
echo "======================================"
echo "Datos HDFS: $OUTPUT_BASE"
echo "Graficos: $RESULTADOS_DIR"
echo "Registro: $LOG"
