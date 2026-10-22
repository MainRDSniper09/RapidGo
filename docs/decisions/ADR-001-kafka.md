# ADR-001 - Estrategia Kafka Variante 6

## Contexto / problema
Definir configuración de Kafka para medir demoras y backlog en la preparación de comercios.

## Opciones consideradas
1. Partición sin key.
2. Usar pedido_id como key.
3. Usar comercio_id como key.

## Decisión tomada
- Topic: preparacion_pedidos
- Key: comercio_id. Garantiza que todos los eventos de un mismo local vayan a la misma partición y mantengan el orden.
- Particiones: 3 particiones iniciales para permitir paralelismo.

## Evidencia utilizada
La variante requiere alertas por acumulación de pedidos en comercios críticos. Ordenar por comercio desde la captura facilita el cálculo de ventanas de tiempo en Spark.

## Consecuencias / trade-offs
Ventajas: Facilita enormemente las agregaciones por comercio.
Limitaciones: Riesgo de "hot partitions" (cuellos de botella) si un comercio grande concentra demasiados pedidos.