"""
Pruebas de la lógica del DAG rapidgo_pipeline (Issue #12).

Se prueban las funciones de airflow/dags/rapidgo_lib/tareas.py sin
necesidad de instalar ni levantar Airflow.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "airflow" / "dags"))

from rapidgo_lib import tareas  # noqa: E402
from producer.generador import GeneradorEventos  # noqa: E402

RUN_ID = "manual__2026-11-04T10:00:00+00:00"


def _escribir_raw(datalake: Path, eventos: list[dict], nombre: str = "part-test.parquet") -> None:
    carpeta = datalake / tareas.CAPA_RAW
    carpeta.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(eventos).to_parquet(carpeta / nombre, index=False)


def _pipeline(datalake: Path) -> dict:
    raw = tareas.verificar_raw(datalake, generar_muestra=False)
    validacion = tareas.validar_datos(datalake, raw["archivos"], RUN_ID)
    calidad = tareas.control_calidad(datalake, validacion, RUN_ID)
    kpis = tareas.consolidar_kpis(datalake, calidad, RUN_ID)
    reporte = tareas.generar_reporte(datalake, RUN_ID, raw, validacion, calidad, kpis)
    return {"raw": raw, "validacion": validacion, "calidad": calidad, "kpis": kpis, "reporte": reporte}


def test_verificar_raw_sin_datos_y_sin_muestra_falla(tmp_path):
    with pytest.raises(FileNotFoundError):
        tareas.verificar_raw(tmp_path, generar_muestra=False)


def test_verificar_raw_genera_muestra_si_esta_vacia(tmp_path):
    resultado = tareas.verificar_raw(tmp_path, generar_muestra=True, cantidad_muestra=50)
    assert resultado["total_registros"] == 50
    assert resultado["muestra_generada"] is not None


def test_pipeline_completo_produce_kpis_y_reporte(tmp_path):
    gen = GeneradorEventos(seed=1)
    _escribir_raw(tmp_path, [gen.generar() for _ in range(300)])

    r = _pipeline(tmp_path)

    assert r["validacion"]["invalidos"] == 0
    assert r["calidad"]["aprobado"] is True
    assert r["kpis"]["total_pedidos"] == 300
    # COM-011 y COM-012 son críticos en el simulador del producer.
    assert {"COM-011", "COM-012"} <= set(r["kpis"]["comercios_criticos"])
    assert (tmp_path / r["reporte"]["ruta_reporte_md"]).exists()
    assert (tmp_path / r["reporte"]["ruta_reporte_json"]).exists()


def test_validacion_envia_invalidos_a_cuarentena(tmp_path):
    gen = GeneradorEventos(seed=2)
    eventos = [gen.generar() for _ in range(10)]
    eventos[0]["pedidos_en_cola"] = -1  # viola el contrato
    _escribir_raw(tmp_path, eventos)

    raw = tareas.verificar_raw(tmp_path, generar_muestra=False)
    validacion = tareas.validar_datos(tmp_path, raw["archivos"], RUN_ID)

    assert validacion["invalidos"] == 1
    assert validacion["ruta_cuarentena"] is not None


def test_calidad_falla_si_hay_demasiados_invalidos(tmp_path):
    gen = GeneradorEventos(seed=3)
    eventos = [gen.generar() for _ in range(10)]
    for e in eventos[:3]:
        e["comercio_id"] = "invalido"
    _escribir_raw(tmp_path, eventos)

    raw = tareas.verificar_raw(tmp_path, generar_muestra=False)
    validacion = tareas.validar_datos(tmp_path, raw["archivos"], RUN_ID)
    with pytest.raises(tareas.CalidadInsuficienteError):
        tareas.control_calidad(tmp_path, validacion, RUN_ID)


def test_calidad_elimina_duplicados_y_marca_alertas(tmp_path):
    gen = GeneradorEventos(seed=4)
    eventos = [gen.generar() for _ in range(40)]
    eventos.append(dict(eventos[0]))  # 1 duplicado (2.4% < 5%)
    _escribir_raw(tmp_path, eventos)

    raw = tareas.verificar_raw(tmp_path, generar_muestra=False)
    validacion = tareas.validar_datos(tmp_path, raw["archivos"], RUN_ID)
    calidad = tareas.control_calidad(tmp_path, validacion, RUN_ID)

    assert calidad["duplicados_eliminados"] == 1
    limpio = pd.read_parquet(tmp_path / calidad["ruta_limpio"])
    assert len(limpio) == 40
    esperado = (limpio["demora_min"] > tareas.UMBRAL_DEMORA_MIN) | (limpio["pedidos_en_cola"] >= tareas.UMBRAL_COLA)
    assert (limpio["alerta_saturacion"] == esperado).all()


def test_lee_raw_con_fechas_tipo_timestamp(tmp_path):
    """Spark puede escribir columnas de fecha como timestamp en lugar de texto."""
    gen = GeneradorEventos(seed=5)
    df = pd.DataFrame([gen.generar() for _ in range(5)])
    for col in ("timestamp_evento", "inicio_preparacion", "fin_preparacion"):
        df[col] = pd.to_datetime(df[col], utc=True)
    carpeta = tmp_path / tareas.CAPA_RAW / "part-00000"
    carpeta.mkdir(parents=True)
    df.to_parquet(carpeta / "part-00000.snappy.parquet", index=False)

    raw = tareas.verificar_raw(tmp_path, generar_muestra=False)
    validacion = tareas.validar_datos(tmp_path, raw["archivos"], RUN_ID)
    assert validacion["validos"] == 5
