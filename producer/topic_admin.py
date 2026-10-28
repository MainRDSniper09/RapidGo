"""
Creación y descripción del topic de Kafka.

El docker-compose tiene KAFKA_AUTO_CREATE_TOPICS_ENABLE=false, así que el
topic debe crearse explícitamente. Hay dos formas equivalentes:

    1. Automática: el servicio `kafka-init` del docker-compose.
    2. Manual:     python -m producer.topic_admin

Uso:
    python -m producer.topic_admin             # crea (si no existe) y describe
    python -m producer.topic_admin --describir # solo describe
"""

from __future__ import annotations

import argparse
import sys

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from producer import config


def crear_topic(admin: AdminClient, topic: str, particiones: int, replicacion: int) -> None:
    futuros = admin.create_topics(
        [NewTopic(topic, num_partitions=particiones, replication_factor=replicacion)]
    )
    for nombre, futuro in futuros.items():
        try:
            futuro.result()
            print(f"[OK] Topic '{nombre}' creado con {particiones} particiones.")
        except KafkaException as exc:
            if exc.args[0].name() == "TOPIC_ALREADY_EXISTS":
                print(f"[INFO] El topic '{nombre}' ya existe.")
            else:
                raise


def describir_topic(admin: AdminClient, topic: str) -> int:
    """Imprime particiones/líder/réplicas y devuelve el número de particiones."""
    metadata = admin.list_topics(topic=topic, timeout=10)
    info = metadata.topics.get(topic)
    if info is None or info.error is not None:
        raise RuntimeError(f"No se pudo describir el topic '{topic}': {info and info.error}")

    print(f"Topic: {topic} | Particiones: {len(info.partitions)}")
    for pid in sorted(info.partitions):
        p = info.partitions[pid]
        print(f"  Partición {pid} | Líder: {p.leader} | Réplicas: {p.replicas} | ISR: {p.isrs}")
    return len(info.partitions)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Administración del topic RapidGo")
    parser.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument("--particiones", type=int, default=config.KAFKA_PARTICIONES)
    parser.add_argument("--replicacion", type=int, default=config.KAFKA_REPLICACION)
    parser.add_argument("--describir", action="store_true", help="Solo describir el topic")
    args = parser.parse_args(argv)

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    if not args.describir:
        crear_topic(admin, args.topic, args.particiones, args.replicacion)
    particiones = describir_topic(admin, args.topic)

    if particiones < 2:
        print("[ERROR] El proyecto exige varias particiones.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
