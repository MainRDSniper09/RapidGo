"""
Generador de eventos de preparación de pedidos (Variante 6).

Este módulo NO depende de Kafka: solo construye y valida eventos.
Así se puede probar con pytest en GitHub Actions sin levantar un broker.

Modelo de simulación
--------------------
Se simula un conjunto fijo de comercios. Algunos son "críticos": preparan
más lento y acumulan más cola. Esto es intencional para que, en el Issue #10,
Spark pueda disparar las alertas de:
    - tiempo de preparación excedido
    - acumulación de pedidos (backlog)

Cada evento representa un pedido cuya preparación acaba de terminar:
    fin_preparacion   = ahora
    inicio_preparacion = ahora - duración simulada
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator

from producer import config

FORMATO_FECHA = "%Y-%m-%dT%H:%M:%SZ"


@dataclass(frozen=True)
class PerfilComercio:
    """Comportamiento simulado de un comercio."""

    comercio_id: str
    minutos_promedio: float  # tiempo medio de preparación
    minutos_desviacion: float
    cola_promedio: int  # pedidos en cola típicos
    critico: bool


# 10 comercios normales + 2 críticos (COM-011 y COM-012).
COMERCIOS: tuple[PerfilComercio, ...] = tuple(
    PerfilComercio(f"COM-{i:03d}", 15.0, 4.0, 3, False) for i in range(1, 11)
) + (
    PerfilComercio("COM-011", 32.0, 6.0, 9, True),
    PerfilComercio("COM-012", 28.0, 7.0, 12, True),
)


def formatear_fecha(fecha: datetime) -> str:
    """datetime -> '2026-10-21T19:25:00Z' (siempre UTC, sin microsegundos)."""
    return fecha.astimezone(timezone.utc).strftime(FORMATO_FECHA)


def parsear_fecha(texto: str) -> datetime:
    """'2026-10-21T19:25:00Z' -> datetime UTC."""
    return datetime.strptime(texto, FORMATO_FECHA).replace(tzinfo=timezone.utc)


def nuevo_ejecucion_id(ahora: datetime | None = None) -> str:
    """Identificador legible de la ejecución: run-AAAAMMDD-HHMMSS-xxxx."""
    ahora = ahora or datetime.now(timezone.utc)
    return f"run-{ahora:%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:4]}"


class GeneradorEventos:
    """
    Genera eventos válidos según el contrato de la Variante 6.

    Se le puede pasar una semilla (seed) para obtener resultados
    reproducibles, útil en pruebas y demostraciones.
    """

    def __init__(
        self,
        equipo_id: str = config.EQUIPO_ID,
        ejecucion_id: str | None = None,
        seed: int | None = None,
    ) -> None:
        self.equipo_id = equipo_id
        self.ejecucion_id = ejecucion_id or nuevo_ejecucion_id()
        self._rnd = random.Random(seed)
        self._secuencia_pedido = self._rnd.randint(1000, 5000)

    def _siguiente_pedido_id(self) -> str:
        self._secuencia_pedido += 1
        return f"PED-{self._secuencia_pedido}"

    def generar(self, ahora: datetime | None = None) -> dict:
        """Construye un evento (dict) listo para serializar a JSON."""
        ahora = (ahora or datetime.now(timezone.utc)).replace(microsecond=0)

        # Los comercios críticos aparecen un poco más (más volumen = más presión).
        pesos = [3 if c.critico else 1 for c in COMERCIOS]
        comercio = self._rnd.choices(COMERCIOS, weights=pesos, k=1)[0]

        # Duración de preparación: normal truncada a mínimo 2 minutos.
        minutos = max(2.0, self._rnd.gauss(comercio.minutos_promedio, comercio.minutos_desviacion))
        inicio = ahora - timedelta(minutes=round(minutos, 1))

        # Cola: variación alrededor del promedio, nunca negativa.
        cola = max(0, comercio.cola_promedio + self._rnd.randint(-3, 4))

        return {
            "evento_id": str(uuid.UUID(int=self._rnd.getrandbits(128), version=4)),
            "equipo_id": self.equipo_id,
            "ejecucion_id": self.ejecucion_id,
            "version_contrato": config.VERSION_CONTRATO,
            "timestamp_evento": formatear_fecha(ahora),
            "pedido_id": self._siguiente_pedido_id(),
            "comercio_id": comercio.comercio_id,
            "inicio_preparacion": formatear_fecha(inicio),
            "fin_preparacion": formatear_fecha(ahora),
            "pedidos_en_cola": cola,
        }


# --------------------------------------------------------------------------
# Validación del contrato
# --------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _validador(ruta_schema: str = str(config.RUTA_SCHEMA)) -> Draft202012Validator:
    schema = json.loads(Path(ruta_schema).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def errores_contrato(evento: dict) -> list[str]:
    """
    Devuelve la lista de errores del evento. Lista vacía = evento válido.

    Además del JSON Schema, valida una regla que el schema no puede
    expresar: fin_preparacion no puede ser anterior a inicio_preparacion.
    """
    errores = [
        f"{'/'.join(map(str, e.path)) or '(raíz)'}: {e.message}"
        for e in _validador().iter_errors(evento)
    ]
    if not errores:
        if parsear_fecha(evento["fin_preparacion"]) < parsear_fecha(evento["inicio_preparacion"]):
            errores.append("fin_preparacion es anterior a inicio_preparacion")
    return errores


def es_valido(evento: dict) -> bool:
    return not errores_contrato(evento)
