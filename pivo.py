# pivo.py
# Módulo com a lógica para calcular Pontos de Pivô a partir de dados históricos.

import pandas as pd

def calculate_pivot_points(historical_data: pd.DataFrame):
    """
    Calcula os Pontos de Pivô a partir de um DataFrame de dados históricos
    que deve conter as colunas 'High', 'Low', e 'Close'.

    Args:
        historical_data (pd.DataFrame): DataFrame com o histórico de preços.

    Returns:
        dict: Um dicionário contendo os 7 pontos de pivô (P, S1-3, R1-3).
    """
    if historical_data.empty or not all(col in historical_data.columns for col in ['High', 'Low', 'Close']):
        return {}

    # Pega a máxima, mínima e fecho do período completo
    high = historical_data['High'].max()
    low = historical_data['Low'].min()
    close = historical_data['Close'].iloc[-1] # O fecho mais recente

    # Fórmula clássica de Pontos de Pivô
    p = (high + low + close) / 3
    r1 = (2 * p) - low
    s1 = (2 * p) - high
    r2 = p + (high - low)
    s2 = p - (high - low)
    r3 = high + 2 * (p - low)
    s3 = low - 2 * (high - p)
    
    # Devolve os resultados num dicionário para fácil utilização
    return {
        'S3': s3, 'S2': s2, 'S1': s1, 'P': p, 'R1': r1, 'R2': r2, 'R3': r3
    }