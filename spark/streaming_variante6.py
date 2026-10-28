import json
from kafka import KafkaConsumer  # O usaremos consumo directo si está disponible, o lectura de socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when, to_timestamp

# 1. Iniciar sesión de Spark (sin paquetes externos conflictivos)
spark = SparkSession.builder \
    .appName("RapidGo_Variante6_PythonKafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Conectando a Kafka desde Python para extraer mensajes...")

# 2. Conectar directamente a Kafka usando el broker interno de Docker ('kafka:19092')
# Nota: Si ejecutas desde fuera del contenedor usa 'localhost:9092', dentro usa 'kafka:19092'
bootstrap_server = 'kafka:19092'
try:
    consumer = KafkaConsumer(
        'preparacion_pedidos',
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
except Exception:
    # Si falla con la red de Docker, probamos con localhost
    consumer = KafkaConsumer(
        'preparacion_pedidos',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

mensajes = []
for msg in consumer:
    mensajes.append(msg.value)

consumer.close()
print(f"Total de mensajes leídos desde Kafka: {len(mensajes)}")

if len(mensajes) > 0:
    # 3. Crear el DataFrame de Spark directamente desde la lista de diccionarios de Python
    df_raw = spark.createDataFrame(mensajes)

    # 4. Aplicar transformaciones y reglas de negocio
    df_transformado = df_raw \
        .withColumn("inicio_ts", to_timestamp(col("inicio_preparacion"))) \
        .withColumn("fin_ts", to_timestamp(col("fin_preparacion"))) \
        .withColumn("demora_seg", unix_timestamp("fin_ts") - unix_timestamp("inicio_ts")) \
        .withColumn("demora_min", col("demora_seg") / 60) \
        .withColumn("alerta_saturacion", when((col("demora_min") > 15) | (col("pedidos_en_cola") >= 10), True).otherwise(False))

    # 5. Guardar los resultados en el Data Lake (Capa Raw en formato Parquet)
    output_path = "/opt/spark/work-dir/datalake/raw/preparacion_pedidos"
    
    df_transformado.write \
        .mode("append") \
        .parquet(output_path)

    print(f"¡Procesamiento exitoso! Datos guardados en formato Parquet en: {output_path}")
    
    # Mostrar un resumen en la consola
    print("Resumen de los datos procesados:")
    df_transformado.select("pedido_id", "comercio_id", "demora_min", "alerta_saturacion").show(10, truncate=False)
else:
    print("No se encontraron mensajes nuevos en el tópico de Kafka.")