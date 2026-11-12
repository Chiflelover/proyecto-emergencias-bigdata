# Linea base del negocio - INDECI

## Problema

El proyecto busca apoyar la revision preventiva de los
84 distritos de La Libertad mediante informacion historica,
climatica y poblacional.

## Contexto institucional

INDECI utiliza EDAN y SINPAD para registrar y consolidar
informacion relacionada con emergencias.

No se dispone de una medicion oficial del tiempo que
emplea INDECI para elaborar una priorizacion distrital.

Tampoco se ha verificado que INDECI utilice un ranking
mensual Top 20 como procedimiento institucional.

## Linea base experimental

Se utiliza un ranking historico que prioriza los distritos
segun las emergencias del mismo mes del anio anterior.

Se compara con la regresion logistica ponderada de Spark ML.

Ambos metodos seleccionan 20 distritos cada mes.

## Resultados

| Periodo | Historico | Modelo IA |
|---------|-----------|-----------|
| 2024    | 53        | 73        |
| 2025    | 93        | 109       |

Los valores representan distrito-mes con emergencia
identificados dentro del Top 20 mensual.

## Indicador principal

Cobertura Top 20 (Recall@20):

Casos positivos identificados dentro del Top 20 mensual
divididos entre todos los casos positivos del periodo.

## Limitaciones

- La referencia es academica, no institucional.
- No se midieron tiempos reales de INDECI.
- El periodo 2025 no constituye una prueba completamente ciega.
- La solucion no reemplaza las decisiones de las autoridades.

## Fuentes institucionales

EDAN:
https://portal.indeci.gob.pe/respuesta/edan/

Herramientas del COEN:
https://portal.indeci.gob.pe/coen/herramientas/
