import os
import glob
import pandas as pd
from datetime import datetime

print("Iniciando procesamiento de la capa Curada (Silver/Processed)...")

# 1. Buscar los archivos Parquet en la capa Raw
raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "datalake", "raw", "preparacion_pedidos"))
parquet_files = glob.glob(os.path.join(raw_dir, "*.parquet"))

if not parquet_files:
    print("No se encontraron archivos Parquet en la capa Raw. Ejecuta primero 'procesar_parquet.py'.")
else:
    # 2. Leer todos los archivos Parquet de la capa Raw en un solo DataFrame
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    print(f"Registros totales cargados desde Raw: {len(df)}")

    # 3. Generar agregaciones de negocio (Métricas por Comercio)
    # CORREGIDO: 'comercio_id' con 'r'
    resumen_comercios = df.groupby('comercio_id').agg(
        total_pedidos=('pedido_id', 'count'),
        tiempo_promedio_preparacion_min=('demora_min', 'mean'),
        tiempo_maximo_preparacion_min=('demora_min', 'max'),
        total_alertas_saturacion=('alerta_saturacion', 'sum')
    ).reset_index()

    # Redondear decimales para mayor prolijidad
    resumen_comercios['tiempo_promedio_preparacion_min'] = resumen_comercios['tiempo_promedio_preparacion_min'].round(2)
    resumen_comercios['tiempo_maximo_preparacion_min'] = resumen_comercios['tiempo_maximo_preparacion_min'].round(2)

    # 4. Guardar los resultados en la Capa Processed / Curated del Data Lake
    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "datalake", "processed", "resumen_comercios"))
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, f"resumen-comercios-{datetime.now().strftime('%Y%m%d-%H%M%S')}.parquet")
    resumen_comercios.to_parquet(output_file, index=False)

    print(f"\n¡Capa Procesada generada con éxito!\nArchivo guardado en: {output_file}")
    
    # 5. Mostrar la tabla resumen en consola
    print("\n--- Resumen de Indicadores por Comercio (Capa Curada) ---")
    print(resumen_comercios.to_string(index=False))