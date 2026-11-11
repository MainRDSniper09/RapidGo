# ADR-002 - Procesamiento con Spark Structured Streaming (Variante 6)

## Contexto / problema
Los eventos de preparación de pedidos llegan a Kafka (`preparacion_pedidos`,
key `comercio_id`, ADR-001) y deben transformarse, evaluarse contra las reglas
de alerta y persistirse en el Data Lake para que Airflow (ADR-003) calcule KPIs.
Las versiones anteriores leían Kafka con `kafka-python` en batch o usaban la
fuente simulada `rate`, y no funcionaban dentro del entorno Docker (Issue #15).

## Opciones consideradas
1. Consumidor Python (`kafka-python`/pandas) en batch.
2. Spark Structured Streaming con la fuente nativa de Kafka.

## Decisión tomada
- `spark/streaming_variante6.py` con `readStream.format("kafka")`
  (conector `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`) en el clúster
  standalone (`spark://spark-master:7077`, 2 workers).
- Contrato aplicado con `from_json` y un schema explícito; los mensajes inválidos van a
  `processed/cuarentena_streaming`.
- Transformaciones: timestamps (`event_time`, `inicio_ts`, `fin_ts`), `demora_min`,
  reglas de alerta (`demora_min > 15`, `pedidos_en_cola >= 10`) y `tipo_alerta`.
- Deduplicación por `evento_id` con `dropDuplicatesWithinWatermark` (watermark de 10 min).
- Agregación por ventana de 5 min y comercio (modo `update` + `foreachBatch`).
- Sinks Parquet: `raw/preparacion_pedidos`, `processed/alertas_streaming`,
  `processed/ventanas_comercio`; checkpoints en `checkpoints/streaming/<consulta>`.
- Dos modos de trigger: `continuo` (cada 10 s) y `disponible` (AvailableNow:
  procesa lo pendiente y termina, para encadenar con Airflow).

## Evidencia utilizada
Ejecución de integración del 2026-11-11 (`docs/evidencias/integracion/`): 300 eventos
procesados en la primera ejecución y solo los 50 nuevos en la segunda (los checkpoints
reanudan sin duplicar). Las alertas de Spark coinciden 1:1 con los KPIs de Airflow.

## Consecuencias / trade-offs
Ventajas: tolerancia a fallos por checkpoints, procesamiento distribuido y
event-time con watermark.

Desventajas: el conector se descarga con `--packages` en la primera ejecución
(requiere internet). Spark no permite encadenar dos operadores con estado en modo
`update`, por eso la agregación por ventana no aplica la deduplicación (el producer
es idempotente, así que los duplicados son excepcionales). Las ventanas se guardan
por micro-batch, y quien las consuma debe quedarse con el mayor `batch_id`.
