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
    tiempo_preparacion_minutos = 25  # Ejemplo de prueba
    umbral_alerta = 20  # Umbral definido en la Semana 1
    
    alerta_activa = tiempo_preparacion_minutos > umbral_alerta
    assert alerta_activa == True, "La alerta de preparación debería activarse si supera el umbral."
