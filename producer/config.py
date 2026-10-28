"""
Configuración del Producer.

Todos los valores se pueden sobreescribir con variables de entorno o con
un archivo .env en la raíz del proyecto (ver .env.example).

Decisiones tomadas en ADR-001:
    - Topic:       preparacion_pedidos
    - Key:         comercio_id
    - Particiones: 3
"""

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv es opcional en tiempo de ejecución
    pass

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent

# --- Kafka -----------------------------------------------------------------
# Desde Windows/host se usa el listener HOST (localhost:9092).
# Desde otro contenedor de la red rapidgo-net se usa kafka:19092.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "preparacion_pedidos")
KAFKA_PARTICIONES = int(os.getenv("KAFKA_PARTICIONES", "3"))
KAFKA_REPLICACION = int(os.getenv("KAFKA_REPLICACION", "1"))

# --- Identificación del equipo ---------------------------------------------
EQUIPO_ID = os.getenv("EQUIPO_ID", "rapidgo-v6")

# --- Contrato ---------------------------------------------------------------
RUTA_SCHEMA = RAIZ_PROYECTO / "samples" / "contrato_evento_variante6.schema.json"
VERSION_CONTRATO = "1.0"
