"""
Pruebas de regresión de las transformaciones del job de Spark (Issue #15).

Se ejecutan en modo batch (sin Kafka) sobre DataFrames con el mismo formato
que entrega la fuente Kafka (key/value en bytes). Requieren Java; si no hay
una JVM disponible las pruebas se omiten.
"""

import json
import shutil
from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java no está disponible: se omiten las pruebas de Spark", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from producer.generador import GeneradorEventos  # noqa: E402
from spark import streaming_variante6 as job  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("tests-rapidgo")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _kafka_df(spark, valores):
    filas = [
        (b"COM-001", v.encode("utf-8"), 0, i, datetime(2026, 11, 11, tzinfo=timezone.utc))
        for i, v in enumerate(valores)
    ]
    return spark.createDataFrame(filas, "key binary, value binary, partition int, offset long, timestamp timestamp")


def _evento(**cambios):
    e = GeneradorEventos(seed=7).generar(datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc))
    e.update(cambios)
    return e


def test_separa_validos_e_invalidos(spark):
    valores = [
        json.dumps(_evento()),
        "esto no es json",
        json.dumps(_evento(pedidos_en_cola=-3)),
        json.dumps(_evento(inicio_preparacion="2026-11-11T12:30:00Z")),  # fin < inicio
    ]
    parseado = job.parsear_kafka(_kafka_df(spark, valores))
    valido = job.condicion_valido()
    assert parseado.filter(valido).count() == 1
    assert parseado.filter(~valido | valido.isNull()).count() == 3


def test_transformaciones_y_reglas_de_alerta(spark):
    casos = [
        _evento(evento_id="00000000-0000-4000-8000-000000000001", inicio_preparacion="2026-11-11T11:40:00Z", pedidos_en_cola=2),
        _evento(evento_id="00000000-0000-4000-8000-000000000002", inicio_preparacion="2026-11-11T11:50:00Z", pedidos_en_cola=12),
        _evento(evento_id="00000000-0000-4000-8000-000000000003", inicio_preparacion="2026-11-11T11:30:00Z", pedidos_en_cola=10),
        _evento(evento_id="00000000-0000-4000-8000-000000000004", inicio_preparacion="2026-11-11T11:50:00Z", pedidos_en_cola=1),
    ]
    parseado = job.parsear_kafka(_kafka_df(spark, [json.dumps(c) for c in casos]))
    filas = {
        r.evento_id[-1]: r
        for r in job.transformar(parseado.filter(job.condicion_valido()), deduplicar=False).collect()
    }

    assert filas["1"].demora_min == 20.0 and filas["1"].tipo_alerta == "TIEMPO_EXCEDIDO"
    assert filas["2"].demora_min == 10.0 and filas["2"].tipo_alerta == "BACKLOG"
    assert filas["3"].tipo_alerta == "TIEMPO_Y_COLA"
    assert filas["4"].alerta_saturacion is False and filas["4"].tipo_alerta == "NINGUNA"


def test_deduplica_por_evento_id(spark, tmp_path):
    """dropDuplicatesWithinWatermark solo existe en streaming: se usa un stream real."""
    e = _evento()
    origen = tmp_path / "origen"
    _kafka_df(spark, [json.dumps(e), json.dumps(e), json.dumps(_evento(evento_id="00000000-0000-4000-8000-0000000000aa"))]).write.parquet(str(origen))

    stream = spark.readStream.schema(spark.read.parquet(str(origen)).schema).parquet(str(origen))
    eventos = job.transformar(job.parsear_kafka(stream).filter(job.condicion_valido()))
    q = (
        eventos.writeStream.format("memory").queryName("dedup_test")
        .option("checkpointLocation", str(tmp_path / "chk"))
        .trigger(availableNow=True).start()
    )
    q.awaitTermination()
    assert spark.table("dedup_test").count() == 2


def test_agregacion_por_ventana(spark):
    casos = [
        _evento(evento_id=f"00000000-0000-4000-8000-00000000000{i}", comercio_id="COM-011",
                inicio_preparacion="2026-11-11T11:30:00Z", pedidos_en_cola=c)
        for i, c in enumerate([5, 11, 3], start=1)
    ]
    parseado = job.parsear_kafka(_kafka_df(spark, [json.dumps(c) for c in casos]))
    eventos = job.transformar(parseado.filter(job.condicion_valido()), deduplicar=False)
    [ventana] = job.agregar_ventanas(eventos).collect()

    assert ventana.comercio_id == "COM-011"
    assert ventana.pedidos == 3
    assert ventana.cola_maxima == 11
    assert ventana.alertas == 3
    assert ventana.ventana_saturada is True
