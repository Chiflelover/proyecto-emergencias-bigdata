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

`src/pipeline_emergencias.py` ejecuta la sección verificable del flujo **RAW → PROCESSED**
que se encontraba distribuida en el notebook original:

1. Limpia SINPAD histórico (normaliza nombres, interpreta formatos de fecha y valida calidad).
2. Filtra La Libertad y enlaza COEN por código SINPAD (LEFT JOIN).
3. Integra CHIRPS por provincia/distrito/año/mes e INEI por provincia/distrito/año.
4. Genera el dataset maestro y el panel distrito-mes (incluyendo meses sin emergencias).
5. Valida cantidades y guarda resultados en Parquet.

Este script **no automatiza todavía** la adquisición externa de los datasets, la
extracción de los PDFs COEN, la preparación de CHIRPS/INEI, el modelo predictivo ni
la generación de gráficos. El notebook contiene estas exploraciones y experimentos.

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

## Prepara los datos

Consulta [data/README.md](data/README.md) para los enlaces y los archivos de entrada.
Antes de ejecutar, comprueba las rutas:

```bash
hdfs dfs -ls /proyecto_emergencias/raw/sinpad
hdfs dfs -ls /proyecto_emergencias/raw/coen
hdfs dfs -ls /proyecto_emergencias/raw/inei
hdfs dfs -ls /proyecto_emergencias/raw/chirps/historico
```

## Ejecución en Debian

En una máquina que **ya tenga Hadoop, YARN y Spark instalados y configurados**:

```bash
start-all.sh
jps
spark-submit --master yarn --deploy-mode client \
  src/pipeline_emergencias.py \
  --output-base hdfs://localhost:9000/proyecto_emergencias/reproduccion_pipeline
```

El script **no sobrescribe** las salidas existentes ni lee los resultados procesados
como entrada: únicamente lee archivos RAW. Si la carpeta de reproducción ya existe,
la escritura falla para proteger los resultados anteriores. Para repetir, usa un
**nuevo** nombre de `--output-base`, por ejemplo `reproduccion_pipeline_v2`.

Salida esperada:

```text
.../reproduccion_pipeline/processed/emergencias_nacional
.../reproduccion_pipeline/processed/emergencias_la_libertad
.../reproduccion_pipeline/processed/emergencias_integradas
.../reproduccion_pipeline/processed/dataset_maestro_2003_2025
.../reproduccion_pipeline/processed/panel_distrito_mes_2003_2025
```

Comprobación:

```bash
hdfs dfs -ls /proyecto_emergencias/reproduccion_pipeline/processed
```

Conteos esperados **para el corte utilizado en el informe**: SINPAD nacional 142139,
La Libertad 2853, CHIRPS histórico 23184, dataset maestro 2853 y panel distrito-mes
23184. El script se detiene si estos conteos no coinciden, para no generar salidas
que aparenten corresponder al corte original.

La ejecución debería mostrar `PIPELINE OK: RAW -> PROCESSED`, pero esta versión
**aún no se ha probado con datos HDFS reales en este entorno de entrega**; debe
validarse en la VM del equipo antes de afirmar reproducibilidad completa.

## Notebook

`notebooks/Proyecto_Emergencias_BigData.ipynb` es el cuaderno exploratorio original
(con salidas borradas en la copia para GitHub). No es un flujo lineal: contiene
reintentos, pruebas, modelos y análisis que dependen de los Parquet que se generaron
en distintas sesiones. El script de `src/` es la ruta de reproducción del pipeline
básico.

## Evidencias y limitaciones

Guarda capturas de las verificaciones HDFS, log de ejecución (sin datos sensibles),
conteos de salida y pruebas de rendimiento en `docs/evidencias/`.
Este es un entorno académico de **un solo nodo**, sin alta disponibilidad.
COEN, CHIRPS e INEI requieren preparación previa y no tienen descarga automatizada.
