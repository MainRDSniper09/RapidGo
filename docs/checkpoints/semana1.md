# Checkpoint Semana 1

## 1. Avance de la semana

Durante la primera semana se realizó la organización inicial del proyecto **RapidGo - Pipeline Big Data End-to-End**.

Se definió la estructura base del repositorio, el flujo de trabajo en GitHub y la distribución de las 12 Issues entre los tres integrantes del equipo. También se creó la estructura inicial para los componentes que se desarrollarán posteriormente, como Kafka, Spark, Data Lake, Airflow, pruebas y documentación.

Además, se dejó preparado un `docker-compose.yml` preliminar, el archivo `requirements.txt` y el `README.md` inicial del proyecto.

## 2. Decisión más importante

La decisión más importante de esta semana fue organizar el proyecto como una solución que irá creciendo durante las cuatro semanas, en lugar de desarrollar cada entrega de forma independiente.

Se decidió utilizar una arquitectura basada en:

```text
Producer
   ↓
Kafka
   ↓
Spark Structured Streaming
   ↓
Data Lake
Raw → Processed → Curated
   ↓
Airflow
   ↓
KPIs / Alertas
```

También se definió que todo cambio funcional deberá estar relacionado con un Issue y desarrollarse mediante una rama y un Pull Request antes de llegar a la rama principal.

Esta decisión permitirá mantener una mejor organización y dejar evidencia clara del trabajo realizado por cada integrante.

## 3. Riesgo principal

El principal riesgo identificado es la integración entre las diferentes tecnologías del proyecto.

Aunque Kafka, Spark, Data Lake y Airflow pueden funcionar por separado, durante las siguientes semanas será necesario garantizar que todos los componentes puedan comunicarse correctamente dentro del entorno Docker.

Otro riesgo importante es realizar cambios directamente sobre el proyecto sin respetar el flujo de Issues, ramas y Pull Requests, ya que esto dificultaría demostrar la participación individual de los integrantes.

## 4. Plan para la Semana 2

Durante la Semana 2 se espera implementar la primera versión funcional del pipeline de streaming.

Las principales actividades serán:

- Implementar el Producer de eventos.
- Configurar Kafka y el topic del proyecto.
- Aplicar la key y el particionamiento definidos por el equipo.
- Conectar Spark Structured Streaming con Kafka.
- Implementar mínimo tres transformaciones.
- Crear una agregación por ventana.
- Implementar las reglas de alerta.
- Guardar resultados en formato Parquet dentro del Data Lake.
- Configurar checkpoints de Spark.
- Crear pruebas automatizadas.
- Activar GitHub Actions para los Pull Requests.

El objetivo de la siguiente semana será obtener el release:

```text
v0.2.0-streaming
```

con un flujo funcional:

```text
Kafka → Spark Structured Streaming → Data Lake
```

## 5. Evidencias relacionadas

- Issue #1 — Foundation del proyecto.
- Issue #2 — Arquitectura y contrato del evento.
- Issue #3 — KPIs y reglas de alerta.
- Milestone `v0.1.0-foundation`.
- GitHub Project / Kanban del equipo.
- Pull Requests de Semana 1.
- Release `v0.1.0-foundation`.