from minio import Minio
from io import BytesIO
import pandas as pd
from config import MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY, MINIO_BUCKET
import json 

def get_minio_client():
    return Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)

def upload_to_minio(object_name, file_path, bucket = MINIO_BUCKET):
    client = get_minio_client()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.fput_object(bucket, object_name, file_path)
    print(f"Arquivo '{file_path}' enviado para o bucket '{bucket}' como '{object_name}'.")

def read_from_minio(bucket_name, object_name):
    client = get_minio_client()
    data = client.get_object(bucket_name, object_name)
    return pd.read_json(BytesIO(data.read()))

def read_from_minio_previsao(bucket_name, object_name):
    client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    # Lê o objeto do MinIO como um JSON
    data = client.get_object(bucket_name, object_name)
    # Parse do conteúdo como JSON
    json_data = json.loads(data.read())
    if isinstance(json_data,dict):
        return [json_data]
    return json_data

def read_from_minio_parquet(bucket_name, object_name):
    client = get_minio_client()
    data = client.get_object(bucket_name, object_name)
    # Lendo o conteúdo Parquet em um DataFrame Pandas
    return pd.read_parquet(BytesIO(data.read()))