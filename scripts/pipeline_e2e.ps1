<#
.SYNOPSIS
    Ejecuta el pipeline RapidGo de punta a punta (Issue #15).

    Producer -> Kafka -> Spark Structured Streaming -> Data Lake -> Airflow -> KPIs / alertas / reporte

.EXAMPLE
    # Desde la raíz del repositorio, con el entorno virtual activado
    .\scripts\pipeline_e2e.ps1
    .\scripts\pipeline_e2e.ps1 -Eventos 500
    .\scripts\pipeline_e2e.ps1 -SinBuild -SinProducer   # solo reprocesa lo que ya hay en Kafka
#>
param(
    [int]$Eventos = 300,
    [switch]$SinBuild,
    [switch]$SinProducer
)

# 'Continue': en PowerShell 5.1 la salida por stderr de docker/spark-submit se
# convierte en error. Los fallos reales se detectan con $LASTEXITCODE (Revisar).
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path $PSScriptRoot -Parent)

function Paso($texto) { Write-Host "`n==> $texto" -ForegroundColor Cyan }
function Revisar($mensaje) { if ($LASTEXITCODE -ne 0) { throw "Falló: $mensaje (exit $LASTEXITCODE)" } }

# 1. Infraestructura -----------------------------------------------------------
Paso '1/6 Levantando infraestructura (Kafka, Spark, Airflow)'
if ($SinBuild) { docker compose up -d } else { docker compose up -d --build }
Revisar 'docker compose up'

Paso 'Esperando a que Airflow esté healthy'
for ($i = 0; $i -lt 60; $i++) {
    $estado = docker inspect -f '{{.State.Health.Status}}' rapidgo-airflow
    if ($estado -eq 'healthy') { break }
    Start-Sleep -Seconds 5
}
if ($estado -ne 'healthy') { throw 'Airflow no quedó healthy en 5 minutos' }
docker compose ps

# 2. Kafka ---------------------------------------------------------------------
Paso '2/6 Verificando topic preparacion_pedidos'
docker exec rapidgo-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic preparacion_pedidos
Revisar 'kafka-topics --describe'

# 3. Producer ------------------------------------------------------------------
if (-not $SinProducer) {
    Paso "3/6 Publicando $Eventos eventos con el producer"
    python -m producer --eventos $Eventos --intervalo 0.02
    Revisar 'producer'
}
docker exec rapidgo-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic preparacion_pedidos

# 4. Spark Structured Streaming ------------------------------------------------
Paso '4/6 Spark Structured Streaming (procesa lo disponible en Kafka y termina)'
docker exec rapidgo-spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --conf spark.executor.memory=512m `
    --conf spark.executor.cores=1 `
    /opt/spark/work-dir/spark/streaming_variante6.py --modo disponible 2>$null |
    Select-String -Pattern '\[RapidGo\]|Exception|Error'
Revisar 'spark-submit'

# 5. Airflow -------------------------------------------------------------------
Paso '5/6 Ejecutando DAG rapidgo_pipeline'
docker exec rapidgo-airflow airflow dags unpause rapidgo_pipeline | Out-Null
$runId = "e2e__$(Get-Date -Format 'yyyyMMddTHHmmss')"
docker exec rapidgo-airflow airflow dags trigger rapidgo_pipeline --run-id $runId | Out-Null
Revisar 'airflow dags trigger'

for ($i = 0; $i -lt 60; $i++) {
    Start-Sleep -Seconds 5
    # En PowerShell 5.1 ConvertFrom-Json no enumera el arreglo: se envuelve en @(...)
    $json = (docker exec rapidgo-airflow airflow dags list-runs rapidgo_pipeline -o json 2>$null) -join ''
    $estado = @($json | ConvertFrom-Json) | ForEach-Object { $_ } |
        Where-Object { $_.run_id -eq $runId } | Select-Object -ExpandProperty state
    if ($estado -in 'success', 'failed') { break }
}
docker exec rapidgo-airflow airflow tasks states-for-dag-run rapidgo_pipeline $runId
if ($estado -ne 'success') { throw "El DAG terminó en estado '$estado'. Revise los logs en http://localhost:8085" }

# 6. Resultado -----------------------------------------------------------------
Paso '6/6 Reporte final'
$reporte = Get-ChildItem datalake\curated\reportes\*.md | Sort-Object LastWriteTime | Select-Object -Last 1
Get-Content $reporte -Encoding UTF8
Write-Host "`nPipeline completado. Reporte: $($reporte.FullName)" -ForegroundColor Green
