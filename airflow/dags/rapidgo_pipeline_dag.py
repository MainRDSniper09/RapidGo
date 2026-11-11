"""
DAG de orquestación del pipeline RapidGo - Variante 6 (Issues #12, #13 y #15).

Coordina las tareas batch y de control posteriores al procesamiento
streaming (Kafka -> Spark -> Data Lake/raw):

    verificar_raw -> validar_datos -> control_calidad -> consolidar_kpis -> generar_reporte -> notificar_alertas

- Cada task devuelve un dict pequeño que Airflow guarda como XCom
  (TaskFlow API) y que la siguiente task recibe como argumento.
- ``notificar_alertas`` además lee XComs explícitamente con ``ti.xcom_pull``.
- La lógica de negocio vive en ``rapidgo_lib/tareas.py`` (sin Airflow,
  probada con pytest).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from rapidgo_lib import tareas

log = logging.getLogger(__name__)

DATALAKE = Path(os.getenv("RAPIDGO_DATALAKE", "/opt/airflow/datalake"))

default_args = {
    "owner": "rapidgo-v6",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    dag_id="rapidgo_pipeline",
    description="Validación, calidad, KPIs y reporte de la preparación de pedidos (Variante 6)",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    is_paused_upon_creation=False,
    tags=["rapidgo", "variante6", "batch", "calidad"],
    params={
        # False por defecto (Issue #15): en el pipeline integrado los datos
        # llegan de Spark; la muestra solo se activa a mano para demos.
        "generar_muestra": Param(
            False,
            type="boolean",
            description="Si la capa Raw está vacía, genera eventos simulados con el producer.",
        ),
        "cantidad_muestra": Param(200, type="integer", minimum=10, maximum=10000),
    },
    doc_md=__doc__,
)
def rapidgo_pipeline():
    @task
    def verificar_raw() -> dict:
        params = get_current_context()["params"]
        resultado = tareas.verificar_raw(
            DATALAKE,
            generar_muestra=params["generar_muestra"],
            cantidad_muestra=params["cantidad_muestra"],
        )
        log.info("Capa Raw: %s archivo(s), %s registros", resultado["total_archivos"], resultado["total_registros"])
        if resultado["muestra_generada"]:
            log.info("Raw estaba vacía: se generó la muestra %s", resultado["muestra_generada"])
        return resultado

    @task
    def validar_datos(raw: dict) -> dict:
        run_id = get_current_context()["run_id"]
        resultado = tareas.validar_datos(DATALAKE, raw["archivos"], run_id)
        log.info(
            "Validación de contrato: %s válidos, %s inválidos (%s%%)",
            resultado["validos"],
            resultado["invalidos"],
            resultado["porcentaje_validos"],
        )
        return resultado

    @task
    def control_calidad(validacion: dict) -> dict:
        run_id = get_current_context()["run_id"]
        try:
            resultado = tareas.control_calidad(DATALAKE, validacion, run_id)
        except tareas.CalidadInsuficienteError as exc:
            # Un problema de calidad no se arregla reintentando: falla sin retries.
            raise AirflowFailException(str(exc)) from exc
        log.info("Checks de calidad: %s", resultado["checks"])
        return resultado

    @task
    def consolidar_kpis(calidad: dict) -> dict:
        run_id = get_current_context()["run_id"]
        resultado = tareas.consolidar_kpis(DATALAKE, calidad, run_id)
        log.info(
            "KPIs: %s comercios, demora promedio %s min, %s%% pedidos con alerta, críticos=%s",
            resultado["comercios"],
            resultado["demora_promedio_global_min"],
            resultado["porcentaje_alertas_global"],
            resultado["comercios_criticos"],
        )
        return resultado

    @task
    def generar_reporte(raw: dict, validacion: dict, calidad: dict, kpis: dict) -> dict:
        run_id = get_current_context()["run_id"]
        resultado = tareas.generar_reporte(DATALAKE, run_id, raw, validacion, calidad, kpis)
        log.info("Reporte generado: %s", resultado["ruta_reporte_md"])
        log.info("\n%s", (DATALAKE / resultado["ruta_reporte_md"]).read_text(encoding="utf-8"))
        return resultado

    @task
    def notificar_alertas() -> None:
        # XCom explícito: se leen los resultados de tasks anteriores por task_id.
        ti = get_current_context()["ti"]
        kpis = ti.xcom_pull(task_ids="consolidar_kpis")
        reporte = ti.xcom_pull(task_ids="generar_reporte")
        if kpis["comercios_criticos"]:
            log.warning(
                "ALERTA RapidGo: %s comercio(s) críticos por saturación: %s. Ver %s",
                len(kpis["comercios_criticos"]),
                ", ".join(kpis["comercios_criticos"]),
                reporte["ruta_reporte_md"],
            )
        else:
            log.info("Sin comercios críticos en esta ejecución.")

    raw = verificar_raw()
    validacion = validar_datos(raw)
    calidad = control_calidad(validacion)
    kpis = consolidar_kpis(calidad)
    reporte = generar_reporte(raw, validacion, calidad, kpis)
    reporte >> notificar_alertas()


rapidgo_pipeline()
