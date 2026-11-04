# Airflow RapidGo - Orquestación del pipeline (Issue #12)

DAG `rapidgo_pipeline`: coordina las tareas **batch y de control** que se
ejecutan después del streaming (Kafka → Spark → `datalake/raw`).

```text
verificar_raw ──► validar_datos ──► control_calidad ──► consolidar_kpis ──► generar_reporte ──► notificar_alertas
      │                 │                  │                                    ▲
      └─────────────────┴──────────────────┴──────── (XCom) ────────────────────┘
```

| # | Task | Tipo | Qué hace | Salida en el Data Lake |
|---|------|------|----------|------------------------|
| 1 | `verificar_raw` | Control | Lista los Parquet de `raw/preparacion_pedidos`. Si está vacía y `generar_muestra=true`, genera eventos con el mismo generador del producer (Issue #9). | `raw/preparacion_pedidos/muestra-airflow-*.parquet` (solo si estaba vacía) |
| 2 | `validar_datos` | **Validación** | Valida cada registro contra el contrato `samples/contrato_evento_variante6.schema.json` (reutiliza `producer.generador.errores_contrato`). | `processed/validados/`, `processed/cuarentena/` |
| 3 | `control_calidad` | **Calidad** | Checks: hay registros, ≥ 95 % válidos, ≤ 5 % duplicados de `evento_id`, sin nulos, demoras en rango 0–240 min. Elimina duplicados y calcula `demora_min` y alertas. Si un check falla, la task falla **sin reintentos** (`AirflowFailException`). | `processed/preparacion_pedidos/` |
| 4 | `consolidar_kpis` | **Agregación** | KPIs por comercio: pedidos, demora promedio / p90 / máxima, cola promedio, alertas de tiempo (> 15 min) y de cola (≥ 10), % de alertas y estado (`CRITICO` > 75 %, `OBSERVACION`, `NORMAL`). | `curated/kpis_comercio/` |
| 5 | `generar_reporte` | **Reporte / salida** | Reporte Markdown + JSON con el resumen de todas las etapas (recibe los XCom de las tasks 1–4). | `curated/reportes/reporte-<run_id>.md` / `.json` |
| 6 | `notificar_alertas` | Salida | Lee XComs con `ti.xcom_pull(task_ids=...)` y registra un `WARNING` con los comercios críticos. | Log de Airflow |

## Configuración relevante

| Requisito del Issue | Dónde |
|---------------------|-------|
| Retries | `default_args["retries"] = 2` |
| retry_delay | `default_args["retry_delay"] = timedelta(minutes=1)` con backoff exponencial (máx. 5 min) |
| XCom | TaskFlow API: cada task devuelve un `dict` (se guarda como XCom y se pasa a la siguiente). `notificar_alertas` usa `ti.xcom_pull` explícito. |
| Dependencias | Definidas por el paso de XCom entre tasks + `generar_reporte >> notificar_alertas` |
| Programación | `@daily`, `catchup=False`, `max_active_runs=1` |
| Parámetros | `generar_muestra` (bool, por defecto `true`), `cantidad_muestra` (int, 200) |

La lógica de negocio está en `dags/rapidgo_lib/tareas.py` (no importa
Airflow) y se prueba con `pytest` en `tests/test_airflow_tareas.py`.
`dags/.airflowignore` evita que Airflow intente parsear esa librería como DAG.

## Estructura

```text
airflow/
├── Dockerfile                           # apache/airflow:3.3.2 + pandas, pyarrow, jsonschema
├── simple_auth_manager_passwords.json   # usuario de laboratorio admin/admin
├── dags/
│   ├── .airflowignore
│   ├── rapidgo_pipeline_dag.py          # definición del DAG
│   └── rapidgo_lib/tareas.py            # lógica de cada task
├── logs/                                # logs de ejecución (no se versionan)
└── plugins/
```

## Levantar Airflow

Airflow se agregó al `docker-compose.yml` con dos servicios:

- `airflow-postgres`: base de metadatos (PostgreSQL 16).
- `airflow`: `airflow standalone` (api-server/UI, scheduler, dag-processor y
  triggerer) con `LocalExecutor`.

```powershell
# Todo el entorno (Kafka + Spark + Airflow)
docker compose up -d --build

# O solo Airflow
docker compose up -d --build airflow

docker compose ps            # rapidgo-airflow debe quedar "healthy" (~1 min)
```

Volúmenes montados en el contenedor:

| Host | Contenedor |
|------|------------|
| `./airflow/dags` | `/opt/airflow/dags` |
| `./datalake` | `/opt/airflow/datalake` (mismo Data Lake que usa Spark) |
| `./producer`, `./samples` | `/opt/airflow/project/...` (contrato y generador) |

## Acceder a la interfaz de Airflow

1. Abrir **http://localhost:8085** (el 8080 lo usa Spark Master).
2. Usuario: `admin` — Contraseña: `admin` (solo laboratorio, definido en
   `airflow/simple_auth_manager_passwords.json`).
3. En **Dags** aparece `rapidgo_pipeline` (tags `rapidgo`, `variante6`).
4. Pestaña **Graph** para ver las dependencias; clic en una task → **Logs**
   para ver su log y **XCom** para ver los valores intercambiados.

## Ejecutar el DAG manualmente

**Desde la UI:** abrir `rapidgo_pipeline` → botón **Trigger** (▶). Se pueden
cambiar los parámetros `generar_muestra` y `cantidad_muestra` antes de
confirmar. El DAG se crea activo (`is_paused_upon_creation=False`); si
aparece pausado, activarlo con el interruptor.

**Desde la terminal:**

```powershell
docker exec rapidgo-airflow airflow dags trigger rapidgo_pipeline

# Con parámetros
docker exec rapidgo-airflow airflow dags trigger rapidgo_pipeline --conf '{\"generar_muestra\": false}'

# Ver estado de las ejecuciones y de cada task
docker exec rapidgo-airflow airflow dags list-runs rapidgo_pipeline
docker exec rapidgo-airflow airflow tasks states-for-dag-run rapidgo_pipeline <run_id>
```

Para procesar datos reales en lugar de la muestra: ejecutar primero el
producer y el job de Spark (que escriben en `datalake/raw/preparacion_pedidos`)
y lanzar el DAG con `generar_muestra=false`.

El reporte queda en `datalake/curated/reportes/reporte-<run_id>.md`.

## Evidencia de ejecución

Ver [`docs/evidencias/airflow/`](../docs/evidencias/airflow/):

- `ejecucion_cli.txt` — DAG listado y ejecuciones en estado `success` (las 6 tasks).
- `dependencias.txt` — grafo de dependencias, retries y retry_delay de cada task.
- `xcom.txt` — valores XCom guardados por cada task.
- `logs_tasks.txt` — logs de las 6 tasks de una ejecución manual.
- `reporte_ejemplo.md` — reporte generado por `generar_reporte`.
- Capturas de la UI (`*.png`): DAG visible, Graph, ejecución exitosa y log de una task
  (se agregan desde la UI siguiendo los pasos anteriores).
