"""
Producer de eventos RapidGo - Variante 6 (Preparación de pedidos).

Genera eventos válidos según el contrato de samples/ y los publica en el
topic de Kafka usando comercio_id como KEY (ADR-001), para que todos los
eventos de un mismo comercio caigan en la misma partición y conserven orden.

Ejemplos (desde la raíz del repositorio):
    python -m producer                         # 50 eventos, 0.5 s entre cada uno
    python -m producer --eventos 200 --intervalo 0.1
    python -m producer --continuo              # hasta Ctrl+C
    python -m producer --eventos 20 --seed 42  # reproducible
    python -m producer --dry-run --eventos 5   # sin Kafka, solo imprime
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from collections import Counter

from producer import config
from producer.generador import GeneradorEventos, errores_contrato


def serializar(evento: dict) -> tuple[bytes, bytes]:
    """Devuelve (key, value) en bytes. La key es el comercio_id (ADR-001)."""
    key = evento["comercio_id"].encode("utf-8")
    value = json.dumps(evento, ensure_ascii=False).encode("utf-8")
    return key, value


def construir_producer(bootstrap: str):
    from confluent_kafka import Producer  # import diferido: --dry-run no necesita Kafka

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": f"{config.EQUIPO_ID}-producer",
            # Durabilidad: el broker confirma solo cuando el mensaje quedó escrito.
            "acks": "all",
            # Evita duplicados si librdkafka reintenta un envío.
            "enable.idempotence": True,
            # Pequeño batching para no enviar mensaje por mensaje.
            "linger.ms": 20,
            # Mismo particionador que el cliente Java de Kafka (murmur2).
            # Así la key -> partición es idéntica a la de kafka-console-producer
            # y cualquier otra herramienta del ecosistema.
            "partitioner": "murmur2_random",
        }
    )


class Estadisticas:
    """Acumula confirmaciones de entrega para el resumen final."""

    def __init__(self) -> None:
        self.enviados = 0
        self.confirmados = 0
        self.fallidos = 0
        self.invalidos = 0
        self.por_particion: Counter[int] = Counter()
        self.comercios_por_particion: dict[int, set[str]] = {}

    def callback(self, err, msg) -> None:
        """librdkafka llama este método cuando el broker confirma (o rechaza)."""
        if err is not None:
            self.fallidos += 1
            print(f"[ERROR] Entrega fallida: {err}", file=sys.stderr)
            return
        self.confirmados += 1
        particion = msg.partition()
        comercio = msg.key().decode("utf-8")
        self.por_particion[particion] += 1
        self.comercios_por_particion.setdefault(particion, set()).add(comercio)
        print(
            f"[OK] topic={msg.topic()} particion={particion} offset={msg.offset()} "
            f"key={comercio}"
        )

    def resumen(self, topic: str, ejecucion_id: str) -> str:
        lineas = [
            "",
            "=" * 64,
            f" RESUMEN EJECUCIÓN {ejecucion_id}",
            "=" * 64,
            f" Topic:        {topic}",
            f" Enviados:     {self.enviados}",
            f" Confirmados:  {self.confirmados}",
            f" Fallidos:     {self.fallidos}",
            f" Inválidos:    {self.invalidos} (descartados por contrato)",
            " Distribución por partición:",
        ]
        for p in sorted(self.por_particion):
            comercios = ", ".join(sorted(self.comercios_por_particion[p]))
            lineas.append(f"   Partición {p}: {self.por_particion[p]:>5} eventos | {comercios}")
        lineas.append("=" * 64)
        return "\n".join(lineas)


def parsear_argumentos(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Producer RapidGo Variante 6")
    parser.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument("--eventos", type=int, default=50, help="Cantidad de eventos")
    parser.add_argument("--intervalo", type=float, default=0.5, help="Segundos entre eventos")
    parser.add_argument("--continuo", action="store_true", help="Enviar hasta Ctrl+C")
    parser.add_argument("--seed", type=int, default=None, help="Semilla reproducible")
    parser.add_argument("--dry-run", action="store_true", help="No envía a Kafka, solo imprime")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parsear_argumentos(argv)
    generador = GeneradorEventos(seed=args.seed)
    stats = Estadisticas()

    print(f"[INFO] Equipo: {generador.equipo_id} | Ejecución: {generador.ejecucion_id}")
    print(f"[INFO] Topic: {args.topic} | Bootstrap: {args.bootstrap} | Key: comercio_id")

    producer = None if args.dry_run else construir_producer(args.bootstrap)

    detener = False

    def _ctrl_c(*_):
        nonlocal detener
        detener = True
        print("\n[INFO] Deteniendo producer...")

    signal.signal(signal.SIGINT, _ctrl_c)

    i = 0
    while not detener and (args.continuo or i < args.eventos):
        i += 1
        evento = generador.generar()

        errores = errores_contrato(evento)
        if errores:
            stats.invalidos += 1
            print(f"[WARN] Evento descartado (contrato): {errores}", file=sys.stderr)
            continue

        key, value = serializar(evento)

        if producer is None:
            print(value.decode("utf-8"))
        else:
            try:
                producer.produce(args.topic, key=key, value=value, on_delivery=stats.callback)
            except BufferError:
                # Cola local llena: esperar a que se vacíe y reintentar una vez.
                producer.poll(1)
                producer.produce(args.topic, key=key, value=value, on_delivery=stats.callback)
            producer.poll(0)  # dispara los callbacks pendientes
        stats.enviados += 1

        if args.intervalo > 0:
            time.sleep(args.intervalo)

    if producer is not None:
        pendientes = producer.flush(15)
        if pendientes:
            print(f"[ERROR] {pendientes} mensajes sin confirmar tras flush.", file=sys.stderr)
            stats.fallidos += pendientes

    print(stats.resumen(args.topic, generador.ejecucion_id))
    return 1 if stats.fallidos else 0


if __name__ == "__main__":
    sys.exit(main())
