import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

print("Inicializando Spark Structured Streaming (Rutas Locales Corregidas)...")

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("RapidGo-SparkStreaming-Pipeline") \
    .master("local[*]") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir esquema del evento
event_schema = StructType([
    StructField("pedido_id", StringType(), True),
    StructField("comercio_id", StringType(), True),
    StructField("inicio_preparacion", StringType(), True),
    StructField("fin_preparacion", StringType(), True),
    StructField("pedidos_en_cola", IntegerType(), True)
])

print("Configurando el origen de streaming...")

# Usamos origen de streaming simulado compatible con Spark 4
df_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 2) \
    .load()

# Definir rutas usando el directorio actual de trabajo dentro del contenedor
current_dir = os.getcwd()
checkpoint_dir = os.path.join(current_dir, "datalake", "checkpoints", "streaming_checkpoint")
output_dir = os.path.join(current_dir, "datalake", "raw", "streaming_output")

# Asegurar que los directorios existan localmente
os.makedirs(checkpoint_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)

print(f"Escribiendo en salida: {output_dir}")
print(f"Escribiendo checkpoints en: {checkpoint_dir}")

query = df_stream.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", output_dir) \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime="5 seconds") \
    .start()

print("\n¡Spark Structured Streaming está corriendo con éxito, usando Checkpoints y escribiendo en el Data Lake!\n")
spark.streams.awaitAnyTermination()