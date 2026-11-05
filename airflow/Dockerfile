# Imagen de Airflow para RapidGo (Issue #12)
# Se agregan las librerías que usan las tasks del DAG.
FROM apache/airflow:3.3.2

# pandas/pyarrow: lectura/escritura Parquet del Data Lake
# jsonschema/python-dotenv: validación del contrato (reutiliza producer/generador.py)
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    pandas \
    pyarrow \
    jsonschema \
    python-dotenv
