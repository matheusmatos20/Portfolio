import requests
import json
import tempfile
from datetime import datetime
from config import API_BASE_URL, TOKEN, HEADERS
from pyspark.sql import DataFrame
import os


def authenticate():
    session = requests.Session()
    session.post(f'{API_BASE_URL}/Login/Autenticar?token={TOKEN}', headers=HEADERS)
    return session

def fetch_data(session, endpoint):
    response = session.get(f'{API_BASE_URL}/{endpoint}')
    return response.json() if response.status_code == 200 else None

def save_json_temp(data):
    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as tmpfile:
        json.dump(data, tmpfile, ensure_ascii=False)
        return tmpfile.name
    
def save_spark_df_to_temp_parquet(df: DataFrame) -> str:
    temp_dir = os.path.join('/tmp', 'spark_temp_' + os.urandom(8).hex())  # Gerando um nome único
    os.makedirs(temp_dir)
    
    # Salvar o DataFrame como Parquet no diretório temporário
    df.write.format("parquet").mode("overwrite").save(temp_dir)
    print(f"Arquivo Parquet temporário gerado em {temp_dir}")

    # Encontrar o caminho do arquivo Parquet gerado dentro do diretório
    parquet_file = None
    for root, dirs, files in os.walk(temp_dir):
        for file in files:
            if file.endswith(".parquet"):
                parquet_file = os.path.join(root, file)
                break
        if parquet_file:
            break

    if parquet_file:
        return parquet_file
    else:
        raise ValueError("Nenhum arquivo Parquet encontrado no diretório temporário.")