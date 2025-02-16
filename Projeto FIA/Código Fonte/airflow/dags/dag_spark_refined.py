from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, input_file_name, col, explode, lit, round, concat, substring, avg, expr
from pyspark.sql.window import Window
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from minio_client import upload_to_minio, read_from_minio, get_minio_client, read_from_minio_previsao
from sptrans_client import save_spark_df_to_temp_parquet
from config import MINIO_BUCKET_TRUSTED, MINIO_BUCKET,MINIO_BUCKET_REFINED
import json
from previsao_schema import schema


# Função para rodar o job Spark
def process_linhas_paradas_previsao():
    # Inicializando a sessão Spark
    spark = (
        SparkSession.builder
        .appName("Projeto2025")
        .getOrCreate()
    )

    # Configurando o cliente MinIO
    client = get_minio_client()

    # Processando 'Linhas' 
    prefix = "Linhas/"  
    objects = client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
    procces_data(objects,spark,prefix)

    print('Lendo linhas')
    # Processando 'Parada'
    prefix = "Parada/"
    objects = client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
    procces_data(objects,spark,prefix)
    print('Lendo parada')
    
    # Processando 'Previsao'
    prefix = "Previsao/"
    objects = client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
    process_data_previsao(objects,spark,prefix)


def process_data_previsao(objects, spark, folder):
    df_list = []
    for obj in objects:
        object_name = obj.object_name
        print(f"Lendo o arquivo: {object_name}")

        # Lendo o arquivo específico em memória
        # df_previsao = read_from_minio_previsao(MINIO_BUCKET, 'Previsao/Previsao_35274_7014416_2025-02-05_14-49-40.json')
        df_previsao = read_from_minio_previsao(MINIO_BUCKET, object_name)
        if df_previsao[0].get('p') == None:
            continue
        df_list.append(*df_previsao)
        print('Leu o arquivo')
        # Convertendo os dados em DataFrame Spark para o processamento
        #df_previsao_spark = spark.read.json(spark.sparkContext.parallelize([json.dumps(record) for record in df_previsao]))
    print(df_list)
    df_grouped = pd.DataFrame.from_records(df_list, columns=["hr", "p"])
    print(df_grouped)
    df_previsao_spark = spark.createDataFrame(df_grouped, schema)
    print(df_previsao_spark)

    df_exploded = df_previsao_spark.select(
            "hr",
            col("p.cp").alias("cp"),
            col("p.np").alias("np"),
            col("p.px").alias("px"),
            col("p.py").alias("py"),
            explode("p.l").alias("l_exploded")
        ).select(
            "hr", "cp", "np", "px", "py", 
            "l_exploded.c", "l_exploded.cl", "l_exploded.sl", 
            "l_exploded.lt0", "l_exploded.lt1", "l_exploded.qv",
            explode("l_exploded.vs").alias("vs_exploded")
        ).select(
            "hr", "cp", "np", "px", "py", "c", "cl", "sl", "lt0", "lt1", "qv",
            "vs_exploded.p", "vs_exploded.t", "vs_exploded.a", "vs_exploded.ta", 
            col("vs_exploded.py").alias("py_vs"), col("vs_exploded.px").alias("px_vs"),
            "vs_exploded.sv", "vs_exploded.is"
        ).dropDuplicates()
    print('Escrevendo o arquivo')
    print(df_exploded.show())
    # Salvando o DataFrame resultante em formato Parquet
    parquet_path = save_spark_df_to_temp_parquet(df_exploded)
    upload_to_minio(f"{folder}{object_name.split('/')[-1].split(".")[0]}.parquet",parquet_path, bucket=MINIO_BUCKET_REFINED)
    print('Arquivo salvo')


def procces_data(objects, spark, folder):
    for obj in objects:
        object_name = obj.object_name
        print(f"Lendo o arquivo: {object_name}")

        # Lendo o arquivo específico em memória
        df_paradas = read_from_minio(MINIO_BUCKET, object_name)
        print('Leu o arquivo')
        # Convertendo os dados em DataFrame Spark para o processamento
        df_paradas_spark = spark.createDataFrame(df_paradas)
        df_paradas_spark = df_paradas_spark.withColumn("file_name", regexp_extract(input_file_name(), r"([^/]+\.json)$", 1))
        df_paradas_spark = df_paradas_spark.dropDuplicates()
        print('Escrevendo o arquivo')
        # Salvando o DataFrame resultante em formato Parquet
        parquet_path = save_spark_df_to_temp_parquet(df_paradas_spark)
        upload_to_minio(f"{folder}{object_name.split('/')[-1].split(".")[0]}.parquet",parquet_path, bucket=MINIO_BUCKET_REFINED)
        print('Arquivo salvo')

# DAG definition
with DAG(
    dag_id="spark_processing_refined",
    start_date=datetime(2025, 2, 5),
    schedule_interval="@daily",  # Defina conforme a necessidade
    catchup=False,
) as dag:
    
    # Task 1: Processar Linhas, Paradas e Previsao
    process_task = PythonOperator(
        task_id="process_linhas_paradas_previsao",
        python_callable=process_linhas_paradas_previsao,
    )

    process_task
