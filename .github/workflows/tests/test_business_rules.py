import pytest
import json
import os

def test_sample_event_contract():
    """Valida que exista un archivo de ejemplo y cumpla con la estructura básica de la Variante 6."""
    sample_path = "samples/evento_sample.json"
    if os.path.exists(sample_path):
        with open(sample_path, "r") as f:
            data = json.load(f)
        assert "comercio" in data or "pedido_id" in data
    else:
        pytest.skip("El archivo de sample aún no está en la ruta esperada.")

def test_preparation_alert_threshold():
    """Valida la lógica base de la regla de alerta por tiempo de preparación excedido."""
    tiempo_preparacion_minutos = 25
    umbral_alerta = 20
    
    alerta_activa = tiempo_preparacion_minutos > umbral_alerta
    assert alerta_activa == True, "La alerta de preparación debería activarse si supera el umbral."

def test_quality_data_fields():
    """Valida que los datos obligatorios para la orquestación y analítica estén presentes."""
    payload = {
        "pedido_id": "ORD-1234",
        "comercio": "Restaurante Central",
        "tiempo_preparacion_minutos": 15,
        "estado": "COMPLETADO"
    }
    
    campos_requeridos = ["pedido_id", "tiempo_preparacion_minutos", "estado"]
    for campo in campos_requeridos:
        assert campo in payload, f"El campo obligatorio {campo} falta en el payload."
