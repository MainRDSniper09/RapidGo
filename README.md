# RapidGo - Pipeline Big Data End-to-End

Proyecto Final de Aula de la asignatura **Línea de Profundización I - Big Data**  
Ingeniería de Sistemas y Computación  
Universidad de Cundinamarca - Extensión Facatativá.

---

## Equipo

- Juan Barreto
- Gabriel Charry
- David Saveedra


---

## Variante

RapidGo dispone de diferentes variantes de negocio para el proyecto final.
Una vez confirmada la variante se documentarán aquí:

- problema de negocio;
- eventos mínimos;
- KPIs;
- reglas de alerta;
- umbrales.

---

## Objetivo

Construir un pipeline Big Data end-to-end capaz de capturar,
transportar, procesar, almacenar y analizar eventos de RapidGo.

La solución integrará Apache Kafka, Apache Spark Structured Streaming,
un Data Lake y Apache Airflow, utilizando Docker como entorno de ejecución
y GitHub para mantener la trazabilidad completa del desarrollo.

---

## Arquitectura

La arquitectura mínima del proyecto es:

```text
FUENTE / GENERADOR DE EVENTOS
            |
            v
          KAFKA
            |
            v
SPARK STRUCTURED STREAMING
            |
            v
         DATA LAKE
    Raw -> Processed -> Curated
            |
            v
          AIRFLOW
    calidad -> agregación -> reporte
            |
            v
     INDICADORES / ALERTAS
```

---

## Ejecución rápida (Semana 2)

```powershell
docker compose up -d                       # Kafka + creación del topic + Spark
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m producer --eventos 100 --intervalo 0.2
pytest -v
```

### Orquestación con Airflow (Semana 3)

```powershell
docker compose up -d --build airflow                       # Airflow + Postgres
docker exec rapidgo-airflow airflow dags trigger rapidgo_pipeline
```

UI: <http://localhost:8085> (usuario `admin` / contraseña `admin`).

| Componente | Documentación |
|------------|---------------|
| Producer / Kafka | [`producer/README.md`](producer/README.md) |
| Contrato del evento | [`samples/contrato_evento_variante6.schema.json`](samples/contrato_evento_variante6.schema.json) |
| Decisión topic / key / particiones | [`docs/decisions/ADR-001-kafka.md`](docs/decisions/ADR-001-kafka.md) |
| Airflow / DAG `rapidgo_pipeline` | [`airflow/README.md`](airflow/README.md) |
| Decisión de orquestación | [`docs/decisions/ADR-003-airflow.md`](docs/decisions/ADR-003-airflow.md) |

