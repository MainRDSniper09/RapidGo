"""
Pruebas del Producer (Issue #9).

No necesitan Kafka: prueban el contrato, la generación de eventos,
la serialización key/value y el conteo de entregas por partición.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from producer import config
from producer.generador import (
    COMERCIOS,
    GeneradorEventos,
    errores_contrato,
    es_valido,
    parsear_fecha,
)
from producer.kafka_producer import Estadisticas, parsear_argumentos, serializar

RAIZ = Path(__file__).resolve().parent.parent
AHORA = datetime(2026, 10, 21, 19, 25, tzinfo=timezone.utc)


@pytest.fixture
def generador():
    return GeneradorEventos(equipo_id="rapidgo-v6", ejecucion_id="run-test", seed=42)


# ----------------------------------------------------------------- contrato
def test_ejemplo_de_samples_cumple_el_contrato():
    evento = json.loads((RAIZ / "samples" / "evento_variante6.json").read_text(encoding="utf-8"))
    assert errores_contrato(evento) == []


def test_eventos_generados_cumplen_el_contrato(generador):
    for _ in range(500):
        evento = generador.generar()
        assert errores_contrato(evento) == [], evento


@pytest.mark.parametrize(
    "campo, valor",
    [
        ("pedidos_en_cola", -1),
        ("pedidos_en_cola", "5"),
        ("comercio_id", "LOCAL-1"),
        ("fin_preparacion", "21/10/2026 19:25"),
        ("version_contrato", "9.9"),
    ],
)
def test_contrato_rechaza_valores_invalidos(generador, campo, valor):
    evento = generador.generar(AHORA)
    evento[campo] = valor
    assert not es_valido(evento)


def test_contrato_rechaza_campos_faltantes_o_extra(generador):
    evento = generador.generar(AHORA)
    del evento["comercio_id"]
    assert not es_valido(evento)

    evento = generador.generar(AHORA)
    evento["campo_no_definido"] = 1
    assert not es_valido(evento)


def test_contrato_rechaza_fin_antes_de_inicio(generador):
    evento = generador.generar(AHORA)
    evento["inicio_preparacion"], evento["fin_preparacion"] = (
        evento["fin_preparacion"],
        evento["inicio_preparacion"],
    )
    assert "fin_preparacion es anterior a inicio_preparacion" in errores_contrato(evento)


# ---------------------------------------------------------------- generador
def test_eventos_identifican_equipo_y_ejecucion(generador):
    evento = generador.generar(AHORA)
    assert evento["equipo_id"] == "rapidgo-v6"
    assert evento["ejecucion_id"] == "run-test"


def test_evento_id_y_pedido_id_son_unicos(generador):
    eventos = [generador.generar(AHORA) for _ in range(300)]
    assert len({e["evento_id"] for e in eventos}) == 300
    assert len({e["pedido_id"] for e in eventos}) == 300


def test_fin_preparacion_es_el_timestamp_del_evento(generador):
    evento = generador.generar(AHORA)
    assert evento["fin_preparacion"] == evento["timestamp_evento"] == "2026-10-21T19:25:00Z"
    assert parsear_fecha(evento["inicio_preparacion"]) < AHORA


def test_misma_semilla_genera_mismos_datos():
    a = GeneradorEventos(ejecucion_id="x", seed=7).generar(AHORA)
    b = GeneradorEventos(ejecucion_id="x", seed=7).generar(AHORA)
    assert a == b


def test_comercios_criticos_preparan_mas_lento(generador):
    """Los comercios críticos deben existir para que Spark pueda disparar alertas."""
    criticos = {c.comercio_id for c in COMERCIOS if c.critico}
    assert criticos, "Debe existir al menos un comercio crítico"

    duraciones = {"critico": [], "normal": []}
    for _ in range(1000):
        e = generador.generar(AHORA)
        minutos = (parsear_fecha(e["fin_preparacion"]) - parsear_fecha(e["inicio_preparacion"])).seconds / 60
        duraciones["critico" if e["comercio_id"] in criticos else "normal"].append(minutos)

    promedio = {k: sum(v) / len(v) for k, v in duraciones.items()}
    assert promedio["critico"] > promedio["normal"] + 10


# ------------------------------------------------------------ serialización
def test_key_del_mensaje_es_comercio_id(generador):
    """ADR-001: la key de Kafka es comercio_id."""
    evento = generador.generar(AHORA)
    key, value = serializar(evento)
    assert key == evento["comercio_id"].encode("utf-8")
    assert json.loads(value) == evento


def test_configuracion_por_defecto_coincide_con_adr_001():
    args = parsear_argumentos([])
    assert args.topic == "preparacion_pedidos"
    assert config.KAFKA_PARTICIONES >= 2


# ------------------------------------------------------- callback de entrega
class MensajeFalso:
    """Imita el objeto Message que confluent_kafka pasa al callback."""

    def __init__(self, particion, offset, key):
        self._p, self._o, self._k = particion, offset, key

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return self._k.encode()

    def topic(self):
        return "preparacion_pedidos"


def test_estadisticas_cuentan_por_particion():
    stats = Estadisticas()
    stats.callback(None, MensajeFalso(0, 10, "COM-001"))
    stats.callback(None, MensajeFalso(0, 11, "COM-001"))
    stats.callback(None, MensajeFalso(2, 5, "COM-011"))
    stats.callback("timeout", None)

    assert stats.confirmados == 3
    assert stats.fallidos == 1
    assert stats.por_particion == {0: 2, 2: 1}
    assert stats.comercios_por_particion[0] == {"COM-001"}
