# Integración end-to-end del pipeline RapidGo (Issue #15)

```text
Producer ──► Kafka ──► Spark Structured Streaming ──► Data Lake ──► Airflow ──► KPIs / alertas / reporte
(python)    preparacion_pedidos   spark/streaming_variante6.py   raw/processed/curated   rapidgo_pipeline
            3 particiones, key=comercio_id
```

## Ejecución rápida (un comando)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

.\scripts\pipeline_e2e.ps1                # levanta todo, 300 eventos, Spark, DAG y muestra el reporte
.\scripts\pipeline_e2e.ps1 -Eventos 500 -SinBuild
```

## Ejecución paso a paso

| # | Paso | Comando | Qué verificar |
|---|------|---------|---------------|
| 1 | Infraestructura | `docker compose up -d --build` | `docker compose ps`: kafka, airflow y airflow-postgres `healthy`; spark-master y los 2 workers `Up`; kafka-init `Exited (0)` |
| 2 | Topic | `docker exec rapidgo-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic preparacion_pedidos` | `PartitionCount: 3` |
| 3 | Producer | `python -m producer --eventos 300 --intervalo 0.02` | Resumen: `Confirmados = Enviados`, `Fallidos: 0`; cada comercio siempre en la misma partición |
| 4 | Recepción en Kafka | `docker exec rapidgo-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic preparacion_pedidos` | Los offsets crecen en lo enviado |
| 5 | Spark | ver bloque siguiente | `[RapidGo] consulta=... mensajes_leidos_kafka=N` en las 4 consultas |
| 6 | Data Lake | `datalake/raw/preparacion_pedidos/*.parquet`, `datalake/processed/{alertas_streaming,ventanas_comercio,cuarentena_streaming}`, `datalake/checkpoints/streaming/*` | Parquet presentes; checkpoints con `offsets/`, `commits/` y `state/` |
| 7 | Airflow | `docker exec rapidgo-airflow airflow dags trigger rapidgo_pipeline` o botón ▶ en <http://localhost:8085> (admin/admin) | Las 6 tasks en `success` |
| 8 | Resultado | `datalake/curated/reportes/reporte-<run_id>.md` | KPIs por comercio; `COM-011` y `COM-012` como `CRITICO` |

Job de Spark (paso 5):

```powershell
docker exec rapidgo-spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --conf spark.executor.memory=512m --conf spark.executor.cores=1 `
    /opt/spark/work-dir/spark/streaming_variante6.py --modo disponible
```

- `--modo disponible` procesa lo pendiente en Kafka y termina (Trigger.AvailableNow), ideal antes de lanzar el DAG.
- `--modo continuo` deja el stream corriendo con micro-batches cada 10 s (Ctrl+C para detener).
- Gracias a los checkpoints, cada ejecución continúa donde quedó la anterior: no reprocesa ni duplica eventos.

## Qué escribe cada componente

| Ruta (`datalake/`) | Escribe | Contenido |
|--------------------|---------|-----------|
| `raw/preparacion_pedidos/` | Spark | Eventos válidos y deduplicados + `event_time`, `inicio_ts`, `fin_ts`, `demora_min`, `alerta_tiempo`, `alerta_cola`, `alerta_saturacion`, `tipo_alerta`, metadatos de Kafka |
| `processed/alertas_streaming/` | Spark | Solo eventos con alerta (demora > 15 min o cola ≥ 10) |
| `processed/ventanas_comercio/` | Spark | Agregación por ventana de 5 min y comercio (`batch_id` mayor = valor vigente) |
| `processed/cuarentena_streaming/` | Spark | Mensajes que no cumplen el contrato |
| `checkpoints/streaming/<consulta>/` | Spark | Offsets, commits y estado de cada consulta |
| `processed/validados/`, `processed/cuarentena/`, `processed/preparacion_pedidos/` | Airflow | Validación de contrato y datos limpios |
| `curated/kpis_comercio/`, `curated/reportes/` | Airflow | KPIs y reporte final (Markdown + JSON) |

## Problemas encontrados y correcciones

| # | Problema | Síntoma | Corrección |
|---|----------|---------|------------|
| 1 | **No existía un job de Spark Structured Streaming conectado a Kafka.** `spark/streaming_variante6.py` usaba `kafka-python` (no está en la imagen de Spark ni en `requirements.txt`) y leía en batch; `spark_streaming_pipeline.py` usa la fuente simulada `rate`. | El eslabón Kafka → Spark → Data Lake no funcionaba dentro de Docker. | `spark/streaming_variante6.py` reescrito con `readStream.format("kafka")` (conector `spark-sql-kafka-0-10_2.13:4.2.0`), contrato, 5 transformaciones, reglas de alerta, deduplicación con watermark, ventanas de 5 min, cuarentena y checkpoints. |
| 2 | Executors de Spark sin recursos. | `WARN TaskSchedulerImpl: Initial job has not accepted any resources` en bucle; el job nunca avanza. | Spark pide 1 GB por executor y cada worker ofrece 768 MB: se envía `spark.executor.memory=512m`, `spark.executor.cores=1`. |
| 3 | **Airflow no podía escribir en el Data Lake creado por Spark.** | `PermissionError: [Errno 13] Permission denied: '/opt/airflow/datalake/processed/validados'` (y 2 reintentos fallidos). | Spark corre con UID 185 y crea carpetas con permisos 755; Airflow corría con UID 50000. Se configura `user: "185:0"` en el servicio `airflow` (la imagen de Airflow admite cualquier UID con grupo 0). |
| 4 | El DAG generaba datos simulados en la capa Raw. | Con `generar_muestra=true` por defecto, la ejecución programada que se dispara al levantar Airflow (Raw todavía vacía) habría escrito eventos de muestra mezclados con los reales. | `generar_muestra` ahora es `false` por defecto; solo se activa a mano para demos. |
| 5 | Mensaje engañoso en el log de Spark. | `cuarentena terminada; filas último batch=300` cuando la cuarentena estaba vacía (era `numInputRows`, mensajes leídos de Kafka). | El log ahora muestra `batches` y `mensajes_leidos_kafka` totales. |
| 6 | 100 eventos de la Semana 2 ya no estaban en Kafka. | Spark leyó 300 de los 400 offsets. | No es un error: la retención por defecto de Kafka (7 días) los borró. `kafka-get-offsets --time -2` muestra que el offset más antiguo coincide con el final anterior. |
| 7 | Una task de Airflow quedó interrumpida al reiniciar el contenedor. | `verificar_raw` en `up_for_retry` con el log vacío después de `Pre Execute`. | Transitorio (contenedor reiniciado en plena ejecución). La ejecución se marcó como `failed` y las siguientes corrieron bien. |
| 8 | Las pruebas de Spark necesitan Java. | — | `tests/test_spark_streaming.py` se omite si no hay Java; el CI instala Java 17 con `actions/setup-java`. |

Scripts anteriores que **no** forman parte del pipeline integrado (se conservan
como referencia histórica): `spark_streaming_pipeline.py` (fuente `rate`),
`procesar_parquet.py` y `procesar_curated.py` (usan `kafka-python` desde el
host y pandas; sus funciones las cubren ahora el job de Spark y el DAG).

## Evidencia (2026-11-11)

En [`docs/evidencias/integracion/`](evidencias/integracion/):

| Archivo | Evidencia |
|---------|-----------|
| `01_servicios.txt` | `docker compose ps` con todos los servicios activos |
| `02_topic.txt` | Topic con 3 particiones y offsets antes/después del producer |
| `03_producer.txt` | Producer: 300 enviados / 300 confirmados, distribución por partición |
| `04_kafka_recepcion.txt` | Mensajes leídos del topic con key y partición |
| `05_spark_streaming.txt` | Job en el clúster (2 executors), ejecución inicial y reanudación desde checkpoints |
| `06_datalake.txt` | Parquet por capa, checkpoints, esquema transformado, alertas y ventanas |
| `07_airflow_dag.txt` | DAG con las 6 tasks en `success` sobre los datos de Spark y logs de cada task |
| `08_kpis_alertas.txt` | Validación cruzada Spark vs Airflow (7/7 controles OK) |
| `09_reporte_final.md` | Reporte generado por el DAG |
| `10_pruebas.txt` | Pruebas de regresión (host + pruebas de Spark dentro del contenedor) |
| `11_script_e2e.txt` | Ejecución completa con `scripts/pipeline_e2e.ps1` |

Resultados principales: 350 eventos → 350 válidos (0 en cuarentena, 0 duplicados) →
calidad aprobada → 12 comercios, demora promedio 20.77 min, 242 pedidos con alerta
(69.14 %) → comercios críticos `COM-011` y `COM-012`, los dos que el simulador
define como saturados.
