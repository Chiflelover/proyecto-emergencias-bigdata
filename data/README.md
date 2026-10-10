# Datos utilizados (no se distribuyen aquí)

El repositorio no incorpora datasets masivos, PDFs originales ni resultados de HDFS.

Fuentes oficiales:
- SINPAD histórico: INDECI — https://portal.indeci.gob.pe/direccion-politicas-y-planes/base-de-datos-de-emergencia-y-danos/
- GeoSINPAD 2026 (fuente para una etapa futura): https://geosinpad.indeci.gob.pe/indeci/rest/services/Emergencias/EMERGENCIAS_SINPAD/MapServer
- COEN: https://portal.indeci.gob.pe/coen/nosotros/
- INEI, cuadro 3.17 La Libertad 2025: https://www.inei.gob.pe/media/MenuRecursivo/publicaciones_digitales/Est/CompDptal2025/LaLibertad2025.pdf
- CHIRPS v3: https://chc.ucsb.edu/data/chirps3

La etapa reproducible presupone que están cargados estos archivos en HDFS:

```text
/proyecto_emergencias/raw/sinpad/BD_2003-2025_EMERGENCIAS.csv
/proyecto_emergencias/raw/coen/metadata_reportes_linkage.csv
/proyecto_emergencias/raw/inei/inei_la_libertad_poblacion_distrital_2021_2025.csv
/proyecto_emergencias/raw/chirps/historico/*.csv
```

**Importante:** el CSV COEN es un archivo derivado de metadatos y vinculación de 514 reportes.
Los PDF originales (315 MB) no se preservaron en HDFS RAW, y ese archivo CSV derivado no se reconstruye automáticamente con este script. Tampoco se reconstruye desde cero aquí el CSV mensual distrital de CHIRPS ni la extracción longitudinal del cuadro INEI: deben obtenerse/prepararse previamente, según el procedimiento de recopilación del equipo. Estos son límites de reproducibilidad que deben declararse.

Los datos replicados 10x/100x se usan exclusivamente para pruebas de rendimiento, nunca para EDA o modelos.
