# Producer RapidGo - Variante 6 (Preparación de pedidos)

Generador de eventos que publica en Kafka los pedidos cuya preparación
terminó en un comercio. Implementa el **Issue #9**.

| Parámetro      | Valor                  | Origen   |
|----------------|------------------------|----------|
| Topic          | `preparacion_pedidos`  | ADR-001  |
| Key            | `comercio_id`          | ADR-001  |
| Particiones    | 3                      | ADR-001  |
| Contrato       | `samples/contrato_evento_variante6.schema.json` | Semana 1 |
| Particionador  | `murmur2_random` (igual al cliente Java de Kafka) | Issue #9 |

## Estructura

```text
producer/
├── __main__.py        # permite: python -m producer
├── config.py          # topic, key, bootstrap, equipo (variables de entorno)
├── generador.py       # crea y valida eventos (no depende de Kafka)
├── kafka_producer.py  # envía a Kafka, imprime partición/offset y resumen
└── topic_admin.py     # crea/describe el topic desde Python (alternativa)
```

## Contrato del evento

```json
{
  "evento_id": "8f14e45f-ceea-4a7b-9c3d-2b1e6a0d7c55",
  "equipo_id": "rapidgo-v6",
  "ejecucion_id": "run-20261021-190000",
  "version_contrato": "1.0",
  "timestamp_evento": "2026-10-21T19:25:00Z",
  "pedido_id": "PED-1029",
  "comercio_id": "COM-084",
  "inicio_preparacion": "2026-10-21T19:00:00Z",
  "fin_preparacion": "2026-10-21T19:25:00Z",
  "pedidos_en_cola": 5
}
```

Se conservaron los 5 campos de la Semana 1 y se agregaron:

- `evento_id`: UUID para detectar duplicados en Spark.
- `equipo_id` / `ejecucion_id`: hacen los eventos identificables por equipo y ejecución.
- `version_contrato`: permite evolucionar el contrato sin romper Spark.
- `timestamp_evento`: event-time para las ventanas de Spark (Issue #10).

Cada evento se valida contra el JSON Schema **antes** de enviarse;
los inválidos se descartan y se cuentan en el resumen.

### Simulación

Se simulan 12 comercios: `COM-001` a `COM-010` normales (~15 min de preparación,
cola ~3) y `COM-011`, `COM-012` **críticos** (~30 min, cola ~9-12, el triple de
volumen). Así Spark tendrá datos para disparar las alertas de *tiempo de
preparación excedido* y *acumulación de pedidos*.

## Ejecución paso a paso

> Comandos para PowerShell (Windows). En Linux/macOS cambia la activación
> del entorno por `source .venv/bin/activate`.

### 1. Levantar Kafka (y crear el topic)

```powershell
docker compose up -d
docker logs rapidgo-kafka-init
```

El servicio `kafka-init` crea el topic con 3 particiones y lo describe.
Salida esperada:

```text
Created topic preparacion_pedidos.
Topic: preparacion_pedidos  PartitionCount: 3  ReplicationFactor: 1 ...
```

Alternativa desde Python (si no se usa `kafka-init`):

```powershell
python -m producer.topic_admin
```

### 2. Preparar el entorno Python

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env   # opcional
```

### 3. Ejecutar el producer

```powershell
# 100 eventos, uno cada 0.2 s, semilla fija (reproducible)
python -m producer --eventos 100 --intervalo 0.2 --seed 6

# Modo continuo (Ctrl+C para detener) - útil mientras corre Spark
python -m producer --continuo --intervalo 0.5

# Sin Kafka: solo genera e imprime eventos
python -m producer --dry-run --eventos 5 --intervalo 0
```

Cada confirmación del broker se imprime así:

```text
[OK] topic=preparacion_pedidos particion=1 offset=42 key=COM-012
```

Al final aparece un resumen con la distribución por partición.

### 4. Verificar en Kafka

```powershell
# Describir el topic (particiones, líder, réplicas)
docker exec rapidgo-kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:9092 --describe --topic preparacion_pedidos

# Último offset de cada partición (cuántos mensajes hay en cada una)
docker exec rapidgo-kafka /opt/kafka/bin/kafka-get-offsets.sh `
  --bootstrap-server localhost:9092 --topic preparacion_pedidos

# Leer 5 eventos mostrando partición, offset y key
docker exec rapidgo-kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 --topic preparacion_pedidos `
  --from-beginning --max-messages 5 `
  --formatter-property print.partition=true `
  --formatter-property print.offset=true `
  --formatter-property print.key=true
```

### 5. Pruebas

```powershell
pytest -v tests/test_producer.py
```

No requieren Kafka, por lo que corren en GitHub Actions.

## Evidencia de ejecución 



- Fecha y `ejecucion_id`:
- Salida de `kafka-init` / `--describe`:
- Resumen del producer (enviados, confirmados, distribución por partición):
- Salida de `kafka-get-offsets.sh`:
- 3 mensajes leídos con partición/offset/key:
- Observación: ¿la distribución por partición está balanceada? (ver riesgo de *hot partition* en ADR-001)
