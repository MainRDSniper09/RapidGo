"""
Spark Structured Streaming - RapidGo Variante 6 (preparación de pedidos).

Lee el topic ``preparacion_pedidos`` de Kafka, aplica el contrato del evento,
transformaciones y reglas de alerta, y escribe en el Data Lake (Parquet) con
checkpoints para tolerancia a fallos.

    Kafka (preparacion_pedidos)
        └─► parseo JSON con el schema del contrato
              ├─► inválidos ............► processed/cuarentena_streaming/
              └─► válidos + transformaciones + dedup (watermark)
                    ├─► raw/preparacion_pedidos/    (todos los eventos enriquecidos; lo consume Airflow)
                    ├─► processed/alertas_streaming/ (solo eventos con alerta)
                    └─► processed/ventanas_comercio/ (agregación por ventana de 5 min y comercio)

Transformaciones:
    1. timestamps:  event_time, inicio_ts, fin_ts (string ISO-8601 -> timestamp)
    2. demora_min:  (fin_ts - inicio_ts) en minutos
    3. alertas:     alerta_tiempo (demora_min > 15), alerta_cola (pedidos_en_cola >= 10),
                    alerta_saturacion (cualquiera de las dos) y tipo_alerta
    4. dedup:       por evento_id dentro de un watermark de 10 minutos
    5. ventana:     5 min por comercio -> pedidos, demora promedio/máx., cola máx., alertas

Ejecución (desde la raíz del repo, con docker compose arriba):

    docker exec rapidgo-spark-master /opt/spark/bin/spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \\
        --conf spark.jars.ivy=/tmp/.ivy2 \\
        /opt/spark/work-dir/spark/streaming_variante6.py --modo disponible

``--modo continuo`` (por defecto) deja el stream corriendo cada 10 s;
``--modo disponible`` procesa todo lo que hay en Kafka y termina
(Trigger.AvailableNow), útil para pruebas de integración y para Airflow.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Reglas de alerta (Semana 1 / Issue #3). Las mismas que usa Airflow.
UMBRAL_DEMORA_MIN = 15
UMBRAL_COLA = 10

VENTANA = "5 minutes"
WATERMARK = "10 minutes"

SCHEMA_EVENTO = StructType(
    [
        StructField("evento_id", StringType()),
        StructField("equipo_id", StringType()),
        StructField("ejecucion_id", StringType()),
        StructField("version_contrato", StringType()),
        StructField("timestamp_evento", StringType()),
        StructField("pedido_id", StringType()),
        StructField("comercio_id", StringType()),
        StructField("inicio_preparacion", StringType()),
        StructField("fin_preparacion", StringType()),
        StructField("pedidos_en_cola", IntegerType()),
    ]
)

FORMATO_FECHA = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RapidGo - Spark Structured Streaming (Variante 6)")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "preparacion_pedidos"))
    p.add_argument("--datalake", default=os.getenv("RAPIDGO_DATALAKE", "/opt/spark/work-dir/datalake"))
    p.add_argument("--modo", choices=["continuo", "disponible"], default="continuo")
    p.add_argument("--intervalo", default="10 seconds", help="trigger en modo continuo")
    return p.parse_args()


# --------------------------------------------------------------------------
# Transformaciones (funciones puras sobre DataFrames, reutilizables en pruebas)
# --------------------------------------------------------------------------
def parsear_kafka(kafka_df: DataFrame) -> DataFrame:
    """value (bytes JSON) -> columnas del contrato + metadatos de Kafka."""
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("json"),
        F.col("partition").alias("kafka_particion"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn("evento", F.from_json("json", SCHEMA_EVENTO))


def condicion_valido():
    e = F.col("evento")
    return (
        e.isNotNull()
        & e.evento_id.isNotNull()
        & e.comercio_id.rlike(r"^COM-\d{3}$")
        & e.pedido_id.rlike(r"^PED-\d+$")
        & e.pedidos_en_cola.isNotNull()
        & (e.pedidos_en_cola >= 0)
        & F.to_timestamp(e.timestamp_evento, FORMATO_FECHA).isNotNull()
        & F.to_timestamp(e.inicio_preparacion, FORMATO_FECHA).isNotNull()
        & F.to_timestamp(e.fin_preparacion, FORMATO_FECHA).isNotNull()
        & (F.to_timestamp(e.fin_preparacion, FORMATO_FECHA) >= F.to_timestamp(e.inicio_preparacion, FORMATO_FECHA))
    )


def transformar(validos: DataFrame, deduplicar: bool = True) -> DataFrame:
    df = validos.select("evento.*", "kafka_particion", "kafka_offset", "kafka_timestamp")
    df = (
        df.withColumn("event_time", F.to_timestamp("timestamp_evento", FORMATO_FECHA))
        .withColumn("inicio_ts", F.to_timestamp("inicio_preparacion", FORMATO_FECHA))
        .withColumn("fin_ts", F.to_timestamp("fin_preparacion", FORMATO_FECHA))
        .withColumn(
            "demora_min",
            F.round((F.unix_timestamp("fin_ts") - F.unix_timestamp("inicio_ts")) / 60.0, 2),
        )
        .withColumn("alerta_tiempo", F.col("demora_min") > UMBRAL_DEMORA_MIN)
        .withColumn("alerta_cola", F.col("pedidos_en_cola") >= UMBRAL_COLA)
        .withColumn("alerta_saturacion", F.col("alerta_tiempo") | F.col("alerta_cola"))
        .withColumn(
            "tipo_alerta",
            F.when(F.col("alerta_tiempo") & F.col("alerta_cola"), "TIEMPO_Y_COLA")
            .when(F.col("alerta_tiempo"), "TIEMPO_EXCEDIDO")
            .when(F.col("alerta_cola"), "BACKLOG")
            .otherwise("NINGUNA"),
        )
        .withColumn("procesado_en", F.current_timestamp())
    )
    df = df.withWatermark("event_time", WATERMARK)
    return df.dropDuplicatesWithinWatermark(["evento_id"]) if deduplicar else df


def agregar_ventanas(eventos: DataFrame) -> DataFrame:
    return (
        eventos.groupBy(F.window("event_time", VENTANA).alias("ventana"), "comercio_id")
        .agg(
            F.count("*").alias("pedidos"),
            F.round(F.avg("demora_min"), 2).alias("demora_promedio_min"),
            F.max("demora_min").alias("demora_maxima_min"),
            F.max("pedidos_en_cola").alias("cola_maxima"),
            F.sum(F.col("alerta_saturacion").cast("int")).alias("alertas"),
        )
        .withColumn(
            "ventana_saturada",
            (F.col("demora_promedio_min") > UMBRAL_DEMORA_MIN) | (F.col("cola_maxima") >= UMBRAL_COLA),
        )
        .select(
            F.col("ventana.start").alias("ventana_inicio"),
            F.col("ventana.end").alias("ventana_fin"),
            "comercio_id",
            "pedidos",
            "demora_promedio_min",
            "demora_maxima_min",
            "cola_maxima",
            "alertas",
            "ventana_saturada",
        )
    )


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main() -> None:
    args = argumentos()
    lake = args.datalake.rstrip("/")
    chk = f"{lake}/checkpoints/streaming"

    spark = (
        SparkSession.builder.appName("RapidGo_Variante6_StructuredStreaming")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parseado = parsear_kafka(kafka_df)
    valido = condicion_valido()
    eventos = transformar(parseado.filter(valido))
    invalidos = parseado.filter(~valido | valido.isNull()).select(
        "kafka_key", "json", "kafka_particion", "kafka_offset", "kafka_timestamp"
    )

    def trigger(writer):
        if args.modo == "disponible":
            return writer.trigger(availableNow=True)
        return writer.trigger(processingTime=args.intervalo)

    consultas = [
        trigger(
            eventos.writeStream.queryName("raw_eventos")
            .format("parquet")
            .option("path", f"{lake}/raw/preparacion_pedidos")
            .option("checkpointLocation", f"{chk}/raw_eventos")
            .outputMode("append")
        ).start(),
        trigger(
            eventos.filter("alerta_saturacion")
            .writeStream.queryName("alertas")
            .format("parquet")
            .option("path", f"{lake}/processed/alertas_streaming")
            .option("checkpointLocation", f"{chk}/alertas")
            .outputMode("append")
        ).start(),
        trigger(
            invalidos.writeStream.queryName("cuarentena")
            .format("parquet")
            .option("path", f"{lake}/processed/cuarentena_streaming")
            .option("checkpointLocation", f"{chk}/cuarentena")
            .outputMode("append")
        ).start(),
    ]

    # Ventanas: Spark no permite encadenar dos operadores con estado (dedup +
    # agregación) en modo "update", por eso aquí se agrega sin dedup (el
    # producer es idempotente, los duplicados son excepcionales).
    # En modo "update" cada micro-batch emite las ventanas que cambiaron.
    # El file sink de Parquet solo admite "append" (que espera a que cierre la
    # ventana), así que se usa foreachBatch y se guarda el batch_id: el valor
    # vigente de cada (ventana, comercio) es el de mayor batch_id.
    def escribir_ventanas(batch_df: DataFrame, batch_id: int) -> None:
        (
            batch_df.withColumn("batch_id", F.lit(batch_id))
            .write.mode("append")
            .parquet(f"{lake}/processed/ventanas_comercio")
        )

    consultas.append(
        trigger(
            agregar_ventanas(transformar(parseado.filter(valido), deduplicar=False))
            .writeStream.queryName("ventanas_comercio")
            .foreachBatch(escribir_ventanas)
            .option("checkpointLocation", f"{chk}/ventanas_comercio")
            .outputMode("update")
        ).start()
    )

    print(f"[RapidGo] {len(consultas)} consultas activas (modo={args.modo}), leyendo {args.topic} de {args.bootstrap}")
    for q in consultas:
        q.awaitTermination()
        # numInputRows = mensajes leídos de Kafka por la consulta (no filas escritas).
        leidos = sum(p.get("numInputRows", 0) for p in q.recentProgress)
        print(f"[RapidGo] consulta={q.name} terminada; batches={len(q.recentProgress)} mensajes_leidos_kafka={leidos}")
    spark.stop()


if __name__ == "__main__":
    main()
