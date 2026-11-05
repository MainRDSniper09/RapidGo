# ADR-003 - Orquestación con Apache Airflow (Variante 6)

## Contexto / problema
Después del streaming (Kafka → Spark → `datalake/raw`) se necesitan tareas
batch que validen los datos, controlen su calidad, consoliden los KPIs de
preparación de pedidos y generen un reporte con las alertas por comercio.
Estas tareas deben ejecutarse en orden, reintentarse ante fallos transitorios
y dejar evidencia de cada ejecución (Issue #12).

## Opciones consideradas
1. Scripts sueltos (`procesar_curated.py`) ejecutados a mano o con cron.
2. Un único job de Spark que haga todo.
3. Apache Airflow con un DAG propio y la lógica en funciones Python.

## Decisión tomada
- Apache Airflow 3.3.2 en Docker (`airflow standalone` + `LocalExecutor` + PostgreSQL 16), UI en `localhost:8085`.
- DAG `rapidgo_pipeline` (`@daily`, `catchup=False`) con 6 tasks:
  `verificar_raw → validar_datos → control_calidad → consolidar_kpis → generar_reporte → notificar_alertas`.
- `retries=2`, `retry_delay=1 min` con backoff exponencial; los fallos de calidad
  usan `AirflowFailException` para **no** reintentar (reintentar no corrige datos malos).
- Comunicación entre tasks por XCom (TaskFlow API). Por XCom solo viajan
  métricas y rutas; los datos viajan por el Data Lake en Parquet.
- La lógica vive en `airflow/dags/rapidgo_lib/tareas.py`, sin depender de
  Airflow, para probarla con pytest en GitHub Actions.
- La validación reutiliza el contrato y la función `errores_contrato` del producer,
  así producer y Airflow aplican exactamente las mismas reglas.
- Un comercio es `CRITICO` cuando más del 75 % de sus pedidos tiene alerta
  (demora > 15 min o cola ≥ 10).

## Evidencia utilizada
- Ejecuciones exitosas de punta a punta documentadas en `docs/evidencias/airflow/`.
- Con los datos simulados, solo `COM-011` y `COM-012` (los comercios críticos del
  simulador) superan el 75 %. Con un 50 % también aparecían comercios normales,
  porque su tiempo medio (~15 min) coincide con el umbral de demora.

## Consecuencias / trade-offs
Ventajas: orden, reintentos, historial de ejecuciones y logs centralizados;
la UI muestra el grafo de dependencias y los XCom.

Desventajas: el contenedor de Airflow + Postgres agrega ~1 GB de RAM al entorno.
`standalone` no es una configuración de producción (un solo contenedor y
usuario `admin/admin`), lo que es aceptable para el laboratorio. Las tasks usan
pandas, que es suficiente para el volumen del proyecto; con volúmenes mayores
convendría lanzar estas etapas como jobs de Spark desde Airflow.
