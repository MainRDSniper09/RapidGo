"""
Lógica de negocio de las tasks del DAG ``rapidgo_pipeline`` (Issue #12).

Este módulo NO importa Airflow: cada función recibe rutas/parámetros y
devuelve un ``dict`` pequeño y serializable a JSON. El DAG solo orquesta
y pasa esos dicts entre tasks mediante XCom. Así la lógica se puede
probar con pytest en GitHub Actions sin levantar Airflow.

Flujo (capas del Data Lake):

    raw/preparacion_pedidos/*.parquet        (Spark / producer)
        -> processed/validados/              (validar_datos)
        -> processed/cuarentena/             (eventos que no cumplen el contrato)
        -> processed/preparacion_pedidos/    (control_calidad: limpio y sin duplicados)
        -> curated/kpis_comercio/            (consolidar_kpis)
        -> curated/reportes/                 (generar_reporte)

Reglas de alerta (Semana 2 / Issue #10, mismas que usa Spark):
    - tiempo de preparación excedido: demora_min > 15
    - acumulación de pedidos (backlog): pedidos_en_cola >= 10
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# --------------------------------------------------------------------------
# Constantes de negocio
# --------------------------------------------------------------------------
UMBRAL_DEMORA_MIN = 15.0
UMBRAL_COLA = 10

# Calidad mínima exigida para continuar el pipeline.
MIN_PORCENTAJE_VALIDOS = 95.0
MAX_PORCENTAJE_DUPLICADOS = 5.0

# Un comercio se marca CRÍTICO si más de este % de sus pedidos tiene alerta.
# Se usa 75% (y no 50%) porque un comercio normal prepara en ~15 min, justo
# en el umbral, y por azar ~50% de sus pedidos lo superan.
UMBRAL_COMERCIO_CRITICO_PCT = 75.0

CAMPOS_CONTRATO = (
    "evento_id",
    "equipo_id",
    "ejecucion_id",
    "version_contrato",
    "timestamp_evento",
    "pedido_id",
    "comercio_id",
    "inicio_preparacion",
    "fin_preparacion",
    "pedidos_en_cola",
)

CAPA_RAW = Path("raw") / "preparacion_pedidos"
CAPA_VALIDADOS = Path("processed") / "validados"
CAPA_CUARENTENA = Path("processed") / "cuarentena"
CAPA_LIMPIO = Path("processed") / "preparacion_pedidos"
CAPA_KPIS = Path("curated") / "kpis_comercio"
CAPA_REPORTES = Path("curated") / "reportes"


class CalidadInsuficienteError(RuntimeError):
    """Los datos no alcanzan los umbrales mínimos de calidad."""


# --------------------------------------------------------------------------
# Utilidades
# --------------------------------------------------------------------------
def _ahora_utc() -> datetime:
    return datetime.now(timezone.utc)


def _nombre_seguro(run_id: str) -> str:
    """'manual__2026-11-04T10:00:00+00:00' -> 'manual__2026-11-04T10_00_00_00_00'."""
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in run_id)


def listar_parquet_raw(datalake: Path) -> list[Path]:
    """Archivos Parquet de la capa Raw (incluye las carpetas part-* de Spark)."""
    carpeta = Path(datalake) / CAPA_RAW
    if not carpeta.exists():
        return []
    return sorted(
        p
        for p in carpeta.rglob("*.parquet")
        if "_spark_metadata" not in p.parts and not p.name.startswith((".", "_"))
    )


def _leer_parquets(rutas: list[Path]) -> pd.DataFrame:
    if not rutas:
        return pd.DataFrame(columns=list(CAMPOS_CONTRATO))
    return pd.concat([pd.read_parquet(r) for r in rutas], ignore_index=True)


def _a_texto_iso(valor) -> str | None:
    """Normaliza fechas (str / Timestamp de Spark) al formato del contrato."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    if isinstance(valor, str):
        return valor
    ts = pd.Timestamp(valor)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def _evento_desde_fila(fila: dict) -> dict:
    """Reconstruye el evento del contrato a partir de una fila de Raw."""
    evento = {}
    for campo in CAMPOS_CONTRATO:
        valor = fila.get(campo)
        if campo in ("timestamp_evento", "inicio_preparacion", "fin_preparacion"):
            valor = _a_texto_iso(valor)
        elif campo == "pedidos_en_cola" and valor is not None and not pd.isna(valor):
            valor = int(valor)
        elif valor is not None and not isinstance(valor, str) and pd.isna(valor):
            valor = None
        if valor is not None:
            evento[campo] = valor
    return evento


# --------------------------------------------------------------------------
# Task 1: verificar la capa Raw (y sembrar datos de muestra si está vacía)
# --------------------------------------------------------------------------
def generar_muestra_raw(datalake: Path, cantidad: int = 200, seed: int | None = 6) -> Path:
    """
    Escribe en Raw un Parquet con eventos simulados usando el mismo
    generador del producer (Issue #9). Sirve para demostrar el DAG cuando
    todavía no se ha ejecutado el streaming.
    """
    from producer.generador import GeneradorEventos  # import diferido

    gen = GeneradorEventos(seed=seed)
    df = pd.DataFrame([gen.generar() for _ in range(cantidad)])
    destino = Path(datalake) / CAPA_RAW
    destino.mkdir(parents=True, exist_ok=True)
    archivo = destino / f"muestra-airflow-{_ahora_utc():%Y%m%d-%H%M%S}.parquet"
    df.to_parquet(archivo, index=False)
    return archivo


def verificar_raw(datalake: Path, generar_muestra: bool = False, cantidad_muestra: int = 200) -> dict:
    datalake = Path(datalake)
    archivos = listar_parquet_raw(datalake)
    muestra = None
    if not archivos:
        if not generar_muestra:
            raise FileNotFoundError(
                f"No hay archivos Parquet en {datalake / CAPA_RAW}. "
                "Ejecute el streaming o active el parámetro 'generar_muestra'."
            )
        muestra = generar_muestra_raw(datalake, cantidad_muestra)
        archivos = listar_parquet_raw(datalake)

    registros = int(sum(len(pd.read_parquet(a, columns=["evento_id"])) for a in archivos))
    return {
        "archivos": [str(a.relative_to(datalake)) for a in archivos],
        "total_archivos": len(archivos),
        "total_registros": registros,
        "muestra_generada": str(muestra.relative_to(datalake)) if muestra else None,
    }


# --------------------------------------------------------------------------
# Task 2: validación de datos contra el contrato
# --------------------------------------------------------------------------
def validar_datos(datalake: Path, archivos: list[str], run_id: str) -> dict:
    from producer.generador import errores_contrato  # mismo contrato que el producer

    datalake = Path(datalake)
    df = _leer_parquets([datalake / a for a in archivos])

    validos, invalidos = [], []
    for fila in df.to_dict(orient="records"):
        evento = _evento_desde_fila(fila)
        errores = errores_contrato(evento)
        if errores:
            invalidos.append({**evento, "errores": "; ".join(errores)})
        else:
            validos.append(evento)

    nombre = _nombre_seguro(run_id)
    ruta_validos = datalake / CAPA_VALIDADOS / f"validados-{nombre}.parquet"
    ruta_validos.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(validos, columns=list(CAMPOS_CONTRATO)).to_parquet(ruta_validos, index=False)

    ruta_cuarentena = None
    if invalidos:
        ruta_cuarentena = datalake / CAPA_CUARENTENA / f"cuarentena-{nombre}.parquet"
        ruta_cuarentena.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(invalidos).astype(str).to_parquet(ruta_cuarentena, index=False)

    total = len(df)
    return {
        "total_leidos": total,
        "validos": len(validos),
        "invalidos": len(invalidos),
        "porcentaje_validos": round(100 * len(validos) / total, 2) if total else 0.0,
        "ruta_validos": str(ruta_validos.relative_to(datalake)),
        "ruta_cuarentena": str(ruta_cuarentena.relative_to(datalake)) if ruta_cuarentena else None,
    }


# --------------------------------------------------------------------------
# Task 3: control de calidad (duplicados, nulos, rangos, umbrales)
# --------------------------------------------------------------------------
def control_calidad(datalake: Path, validacion: dict, run_id: str) -> dict:
    datalake = Path(datalake)
    df = pd.read_parquet(datalake / validacion["ruta_validos"])

    total = len(df)
    nulos = int(df[list(CAMPOS_CONTRATO)].isna().sum().sum())
    duplicados = int(df.duplicated(subset=["evento_id"]).sum())
    limpio = df.drop_duplicates(subset=["evento_id"]).copy()

    inicio = pd.to_datetime(limpio["inicio_preparacion"], utc=True)
    fin = pd.to_datetime(limpio["fin_preparacion"], utc=True)
    limpio["demora_min"] = ((fin - inicio).dt.total_seconds() / 60).round(2)
    limpio["alerta_tiempo"] = limpio["demora_min"] > UMBRAL_DEMORA_MIN
    limpio["alerta_cola"] = limpio["pedidos_en_cola"] >= UMBRAL_COLA
    limpio["alerta_saturacion"] = limpio["alerta_tiempo"] | limpio["alerta_cola"]

    demoras_fuera_rango = int(((limpio["demora_min"] < 0) | (limpio["demora_min"] > 240)).sum())

    pct_validos = float(validacion["porcentaje_validos"])
    pct_duplicados = round(100 * duplicados / total, 2) if total else 0.0

    checks = {
        "hay_registros": total > 0,
        "porcentaje_validos_ok": pct_validos >= MIN_PORCENTAJE_VALIDOS,
        "duplicados_ok": pct_duplicados <= MAX_PORCENTAJE_DUPLICADOS,
        "sin_nulos": nulos == 0,
        "demoras_en_rango": demoras_fuera_rango == 0,
    }
    resultado = {
        "registros_entrada": total,
        "registros_limpios": len(limpio),
        "duplicados_eliminados": duplicados,
        "porcentaje_duplicados": pct_duplicados,
        "nulos": nulos,
        "demoras_fuera_rango": demoras_fuera_rango,
        "checks": checks,
        "aprobado": all(checks.values()),
    }

    if not resultado["aprobado"]:
        fallidos = [k for k, ok in checks.items() if not ok]
        raise CalidadInsuficienteError(f"Calidad insuficiente, checks fallidos: {fallidos}. Detalle: {resultado}")

    ruta = datalake / CAPA_LIMPIO / f"limpio-{_nombre_seguro(run_id)}.parquet"
    ruta.parent.mkdir(parents=True, exist_ok=True)
    limpio.to_parquet(ruta, index=False)
    resultado["ruta_limpio"] = str(ruta.relative_to(datalake))
    return resultado


# --------------------------------------------------------------------------
# Task 4: agregación / consolidación de KPIs por comercio
# --------------------------------------------------------------------------
def consolidar_kpis(datalake: Path, calidad: dict, run_id: str) -> dict:
    datalake = Path(datalake)
    df = pd.read_parquet(datalake / calidad["ruta_limpio"])

    kpis = (
        df.groupby("comercio_id")
        .agg(
            total_pedidos=("pedido_id", "count"),
            demora_promedio_min=("demora_min", "mean"),
            demora_p90_min=("demora_min", lambda s: s.quantile(0.9)),
            demora_maxima_min=("demora_min", "max"),
            cola_promedio=("pedidos_en_cola", "mean"),
            alertas_tiempo=("alerta_tiempo", "sum"),
            alertas_cola=("alerta_cola", "sum"),
            alertas_saturacion=("alerta_saturacion", "sum"),
        )
        .reset_index()
    )
    kpis["porcentaje_alertas"] = 100 * kpis["alertas_saturacion"] / kpis["total_pedidos"]
    kpis["estado"] = kpis["porcentaje_alertas"].map(
        lambda p: "CRITICO" if p > UMBRAL_COMERCIO_CRITICO_PCT else ("OBSERVACION" if p > 0 else "NORMAL")
    )
    for col in ("demora_promedio_min", "demora_p90_min", "demora_maxima_min", "cola_promedio", "porcentaje_alertas"):
        kpis[col] = kpis[col].astype(float).round(2)
    for col in ("total_pedidos", "alertas_tiempo", "alertas_cola", "alertas_saturacion"):
        kpis[col] = kpis[col].astype(int)
    kpis = kpis.sort_values(["porcentaje_alertas", "demora_promedio_min"], ascending=False)

    ruta = datalake / CAPA_KPIS / f"kpis-{_nombre_seguro(run_id)}.parquet"
    ruta.parent.mkdir(parents=True, exist_ok=True)
    kpis.to_parquet(ruta, index=False)

    total = int(kpis["total_pedidos"].sum())
    alertas = int(kpis["alertas_saturacion"].sum())
    return {
        "ruta_kpis": str(ruta.relative_to(datalake)),
        "comercios": len(kpis),
        "total_pedidos": total,
        "demora_promedio_global_min": round(float(df["demora_min"].mean()), 2) if total else 0.0,
        "total_alertas": alertas,
        "porcentaje_alertas_global": round(100 * alertas / total, 2) if total else 0.0,
        "comercios_criticos": kpis.loc[kpis["estado"] == "CRITICO", "comercio_id"].tolist(),
    }


# --------------------------------------------------------------------------
# Task 5: generación de reporte / salida
# --------------------------------------------------------------------------
def generar_reporte(datalake: Path, run_id: str, raw: dict, validacion: dict, calidad: dict, kpis: dict) -> dict:
    datalake = Path(datalake)
    tabla = pd.read_parquet(datalake / kpis["ruta_kpis"])

    nombre = _nombre_seguro(run_id)
    carpeta = datalake / CAPA_REPORTES
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta_md = carpeta / f"reporte-{nombre}.md"
    ruta_json = carpeta / f"reporte-{nombre}.json"

    filas = "\n".join(
        f"| {r.comercio_id} | {r.total_pedidos} | {r.demora_promedio_min} | {r.demora_p90_min} "
        f"| {r.cola_promedio} | {r.alertas_tiempo} | {r.alertas_cola} | {r.porcentaje_alertas}% | {r.estado} |"
        for r in tabla.itertuples()
    )
    criticos = ", ".join(kpis["comercios_criticos"]) or "ninguno"
    markdown = f"""# Reporte RapidGo - Preparación de pedidos

- **Run:** `{run_id}`
- **Generado (UTC):** {_ahora_utc():%Y-%m-%d %H:%M:%S}

## Resumen del pipeline

| Etapa | Resultado |
|-------|-----------|
| Raw | {raw["total_archivos"]} archivo(s), {raw["total_registros"]} registros |
| Validación | {validacion["validos"]} válidos / {validacion["invalidos"]} inválidos ({validacion["porcentaje_validos"]}%) |
| Calidad | {calidad["registros_limpios"]} limpios, {calidad["duplicados_eliminados"]} duplicados eliminados, aprobado = {calidad["aprobado"]} |
| KPIs | {kpis["comercios"]} comercios, {kpis["total_pedidos"]} pedidos |

## Indicadores globales

- Demora promedio: **{kpis["demora_promedio_global_min"]} min** (umbral {UMBRAL_DEMORA_MIN} min)
- Pedidos con alerta de saturación: **{kpis["total_alertas"]}** ({kpis["porcentaje_alertas_global"]}%)
- Comercios críticos (> {UMBRAL_COMERCIO_CRITICO_PCT}% de pedidos con alerta): **{criticos}**

## KPIs por comercio

| Comercio | Pedidos | Demora prom. (min) | Demora p90 (min) | Cola prom. | Alertas tiempo | Alertas cola | % alertas | Estado |
|----------|---------|--------------------|------------------|------------|----------------|--------------|-----------|--------|
{filas}
"""
    ruta_md.write_text(markdown, encoding="utf-8")

    resumen = {
        "run_id": run_id,
        "raw": raw,
        "validacion": validacion,
        "calidad": calidad,
        "kpis": kpis,
    }
    ruta_json.write_text(json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "ruta_reporte_md": str(ruta_md.relative_to(datalake)),
        "ruta_reporte_json": str(ruta_json.relative_to(datalake)),
        "comercios_criticos": kpis["comercios_criticos"],
    }
