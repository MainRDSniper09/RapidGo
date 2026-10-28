import os
import json
from kafka import KafkaConsumer
import pandas as pd
from datetime import datetime

print("Conectando a Kafka para extraer los mensajes acumulados...")

# Creamos el consumidor apuntando a Kafka en Docker expuesto en localhost:9092
# consumer_timeout_ms=3000 hace que si en 3 segundos no hay más mensajes nuevos, se detenga solo automáticamente.
consumer = KafkaConsumer(
    'preparacion_pedidos',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='grupo-procesamiento-parquet',
    consumer_timeout_ms=3000
)

mensajes = []
for msg in consumer:
    try:
        # Decodificar el mensaje JSON de Kafka
        evento = json.loads(msg.value.decode('utf-8'))
        mensajes.append(evento)
    except Exception as ex:
        print(f"Error decodificando mensaje: {ex}")

consumer.close()
print(f"Total de mensajes leídos desde Kafka: {len(mensajes)}")

if len(mensajes) > 0:
    # 1. Convertir los eventos a un DataFrame de Pandas
    df = pd.DataFrame(mensajes)

    # 2. Aplicar transformaciones y reglas de negocio
    df['inicio_ts'] = pd.to_datetime(df['inicio_preparacion'])
    df['fin_ts'] = pd.to_datetime(df['fin_preparacion'])
    
    # Calcular demora en segundos y minutos
    df['demora_seg'] = (df['fin_ts'] - df['inicio_ts']).dt.total_seconds()
    df['demora_min'] = df['demora_seg'] / 60.0

    # Regla de alerta de saturación (demora > 15 min o cola >= 10 pedidos)
    df['alerta_saturacion'] = (df['demora_min'] > 15) | (df['pedidos_en_cola'] >= 10)

    # 3. Asegurar que la carpeta del Data Lake exista localmente
    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "datalake", "raw", "preparacion_pedidos"))
    os.makedirs(output_dir, exist_ok=True)

    # 4. Guardar los resultados en formato Parquet
    output_file = os.path.join(output_dir, f"part-{datetime.now().strftime('%Y%m%d-%H%M%S')}.parquet")
    
    df.to_parquet(output_file, index=False)
    print(f"\n¡Procesamiento exitoso! Archivo Parquet guardado en:\n{output_file}")
    
    # Mostrar un resumen en consola
    print("\nResumen de los primeros pedidos procesados:")
    print(df[['pedido_id', 'comercio_id', 'demora_min', 'alerta_saturacion']].head(10).to_string(index=False))
else:
    print("No se encontraron mensajes nuevos en el tópico de Kafka.")