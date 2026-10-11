# Análisis y priorización de emergencias en La Libertad — Big Data

Proyecto académico (UPAO) de apoyo a la gestión del riesgo de desastres con datos de
INDECI/SINPAD, COEN, CHIRPS e INEI. El procesamiento se realiza con Spark sobre YARN
y almacena salidas Parquet en HDFS. La organización de referencia es INDECI; este
repositorio **no es un producto ni una publicación oficial de INDECI**.

## Tecnologías usadas

- Debian 12 (VM local, un nodo)
- Hadoop/HDFS 3.3.6, YARN
- Spark 3.5.1 / PySpark; Java 17
- Python 3.11, JupyterLab (para experimentación)

## Qué está automatizado

El archivo `run_pipeline.sh` ejecuta tres etapas:

1. Procesamiento RAW → PROCESSED con Spark y Parquet en HDFS.
2. Entrenamiento y evaluación del modelo base de Regresión Logística.
3. Generación de cuatro gráficos del análisis exploratorio (EDA).

Los scripts utilizados son:

- `src/pipeline_emergencias.py`
- `src/modelo_base.py`
- `src/eda_emergencias.py`

La descarga y preparación de los datasets se realiza previamente,
siguiendo las instrucciones de este README y `data/README.md`.


## Preparación del entorno

Verifica las versiones en Debian:

```bash
java -version
hadoop version
spark-submit --version
python3 --version
```

Se requiere Hadoop/HDFS y YARN configurados en la VM, con `localhost:9000` como NameNode.
Si aún no se han instalado, consulta las guías de instalación de
[Hadoop 3.3.6](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/SingleCluster.html)
y [Spark 3.5.1](https://spark.apache.org/docs/3.5.1/).
El archivo `requirements.txt` documenta las dependencias Python del notebook;
no sustituye la instalación del clúster Spark/Hadoop.

Para instalar las dependencias Python, desde el repositorio:

    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install -r requirements.txt
    export PYSPARK_PYTHON="$(command -v python)"

En Debian puede ser necesario instalar previamente `python3-venv`.
Mantener este entorno activado al ejecutar `./run_pipeline.sh`.


## Descargar los datos preparados

El paquete de entrada está disponible públicamente:

[Descargar datos de entrada (GitHub Releases)](https://github.com/Chiflelover/proyecto-emergencias-bigdata/releases/tag/v1.0-s7)

Descarga `datos_entrada_emergencias.tar.gz` desde la sección
**Assets** del Release.

El paquete contiene los archivos SINPAD, COEN, INEI,
CHIRPS y GeoSINPAD 2026 utilizados en el proyecto.

Sigue las instrucciones de `data/README.md` para cargarlo
en HDFS antes de ejecutar el pipeline.

## Prepara los datos

Consulta [data/README.md](data/README.md) para los enlaces y los archivos de entrada.
Con Hadoop y YARN iniciados (`start-all.sh`), comprueba las rutas:

```bash
hdfs dfs -ls /proyecto_emergencias/raw/sinpad
hdfs dfs -ls /proyecto_emergencias/raw/coen
hdfs dfs -ls /proyecto_emergencias/raw/inei
hdfs dfs -ls /proyecto_emergencias/raw/chirps/historico
```

## Ejecución en Debian

Después de instalar y configurar Hadoop, YARN, Spark, Python y
los datasets indicados en `data/README.md`, iniciar los servicios:

    start-all.sh
    jps

Desde la carpeta del repositorio, ejecutar:

    chmod +x run_pipeline.sh
    ./run_pipeline.sh

El comando procesa los datos, entrena el modelo base y genera
automáticamente los cuatro gráficos, sin abrir JupyterLab.

Los resultados se guardan en:

- HDFS: `/proyecto_emergencias/ejecucion_YYYYMMDD_HHMMSS/processed/`
- Gráficos: `resultados/ejecucion_YYYYMMDD_HHMMSS/`
- Logs: `~/pipeline_logs/`

El modelo imprime su matriz de confusión y métricas en la terminal
y en el log correspondiente.

La ejecución completa fue probada exitosamente en la VM Debian 12
del equipo. Para reproducirla en otra máquina se requiere la misma
estructura de datos y un entorno compatible.

El pipeline valida conteos del dataset histórico original. No está
preparado todavía para procesar automáticamente cortes actualizados.


## Notebook

`notebooks/Proyecto_Emergencias_BigData.ipynb` es el cuaderno exploratorio original
(con salidas borradas en la copia para GitHub). No es un flujo lineal: contiene
reintentos, pruebas, modelos y análisis que dependen de los Parquet que se generaron
en distintas sesiones. La reproducción automatizada se realiza con `./run_pipeline.sh`,
que ejecuta los tres scripts de `src/`.

## Evidencias y limitaciones

Guarda capturas de las verificaciones HDFS, log de ejecución (sin datos sensibles),
conteos de salida y pruebas de rendimiento en `docs/evidencias/`.
Este es un entorno académico de **un solo nodo**, sin alta disponibilidad.
Los datos preparados están disponibles en GitHub Releases. Reconstruirlos desde las fuentes originales requiere preparación adicional.
