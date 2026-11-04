# Reporte RapidGo - Preparación de pedidos

- **Run:** `manual__2026-11-04T23:19:02.611215+00:00`
- **Generado (UTC):** 2026-11-04 23:19:19

## Resumen del pipeline

| Etapa | Resultado |
|-------|-----------|
| Raw | 1 archivo(s), 200 registros |
| Validación | 200 válidos / 0 inválidos (100.0%) |
| Calidad | 200 limpios, 0 duplicados eliminados, aprobado = True |
| KPIs | 12 comercios, 200 pedidos |

## Indicadores globales

- Demora promedio: **21.31 min** (umbral 15.0 min)
- Pedidos con alerta de saturación: **132** (66.0%)
- Comercios críticos (> 75.0% de pedidos con alerta): **COM-011, COM-012**

## KPIs por comercio

| Comercio | Pedidos | Demora prom. (min) | Demora p90 (min) | Cola prom. | Alertas tiempo | Alertas cola | % alertas | Estado |
|----------|---------|--------------------|------------------|------------|----------------|--------------|-----------|--------|
| COM-011 | 43 | 32.78 | 40.66 | 9.23 | 43 | 19 | 100.0% | CRITICO |
| COM-012 | 45 | 28.54 | 37.1 | 12.29 | 43 | 39 | 97.78% | CRITICO |
| COM-006 | 9 | 16.38 | 21.68 | 3.44 | 6 | 0 | 66.67% | OBSERVACION |
| COM-005 | 10 | 15.81 | 21.75 | 3.9 | 6 | 0 | 60.0% | OBSERVACION |
| COM-007 | 12 | 14.45 | 18.64 | 4.33 | 7 | 0 | 58.33% | OBSERVACION |
| COM-008 | 14 | 15.09 | 18.09 | 3.29 | 7 | 0 | 50.0% | OBSERVACION |
| COM-009 | 11 | 12.93 | 17.4 | 4.36 | 5 | 0 | 45.45% | OBSERVACION |
| COM-004 | 9 | 14.13 | 16.3 | 3.56 | 4 | 0 | 44.44% | OBSERVACION |
| COM-010 | 16 | 14.36 | 20.6 | 3.75 | 7 | 0 | 43.75% | OBSERVACION |
| COM-001 | 9 | 13.3 | 16.22 | 3.22 | 2 | 0 | 22.22% | OBSERVACION |
| COM-002 | 11 | 12.38 | 15.0 | 3.0 | 1 | 0 | 9.09% | OBSERVACION |
| COM-003 | 11 | 11.08 | 14.2 | 3.09 | 0 | 0 | 0.0% | NORMAL |
