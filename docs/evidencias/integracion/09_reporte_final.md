# Reporte RapidGo - Preparación de pedidos

- **Run:** `manual__2026-11-11T22:49:48.298196+00:00`
- **Generado (UTC):** 2026-11-11 22:50:18

## Resumen del pipeline

| Etapa | Resultado |
|-------|-----------|
| Raw | 8 archivo(s), 350 registros |
| Validación | 350 válidos / 0 inválidos (100.0%) |
| Calidad | 350 limpios, 0 duplicados eliminados, aprobado = True |
| KPIs | 12 comercios, 350 pedidos |

## Indicadores globales

- Demora promedio: **20.77 min** (umbral 15.0 min)
- Pedidos con alerta de saturación: **242** (69.14%)
- Comercios críticos (> 75.0% de pedidos con alerta): **COM-011, COM-012**

## KPIs por comercio

| Comercio | Pedidos | Demora prom. (min) | Demora p90 (min) | Cola prom. | Alertas tiempo | Alertas cola | % alertas | Estado |
|----------|---------|--------------------|------------------|------------|----------------|--------------|-----------|--------|
| COM-011 | 53 | 32.54 | 40.28 | 9.83 | 53 | 31 | 100.0% | CRITICO |
| COM-012 | 75 | 28.7 | 36.06 | 12.37 | 74 | 61 | 100.0% | CRITICO |
| COM-001 | 14 | 17.02 | 21.29 | 3.29 | 10 | 0 | 71.43% | OBSERVACION |
| COM-006 | 23 | 16.3 | 20.58 | 3.52 | 15 | 0 | 65.22% | OBSERVACION |
| COM-009 | 20 | 15.44 | 19.0 | 3.25 | 13 | 0 | 65.0% | OBSERVACION |
| COM-003 | 30 | 15.37 | 21.23 | 3.2 | 17 | 0 | 56.67% | OBSERVACION |
| COM-008 | 19 | 15.09 | 18.56 | 2.95 | 10 | 0 | 52.63% | OBSERVACION |
| COM-007 | 30 | 15.57 | 20.78 | 3.27 | 15 | 0 | 50.0% | OBSERVACION |
| COM-004 | 26 | 15.1 | 18.95 | 3.42 | 13 | 0 | 50.0% | OBSERVACION |
| COM-005 | 25 | 15.01 | 20.92 | 4.12 | 12 | 0 | 48.0% | OBSERVACION |
| COM-002 | 19 | 13.31 | 16.76 | 4.0 | 5 | 0 | 26.32% | OBSERVACION |
| COM-010 | 16 | 14.52 | 18.05 | 2.81 | 4 | 0 | 25.0% | OBSERVACION |
