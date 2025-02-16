
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from minio_client import get_minio_client, read_from_minio, read_from_minio_parquet, upload_to_minio
from minio import Minio
from io import BytesIO
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, expr, lag, round, floor, concat, lpad, lit, explode, regexp_extract,
    input_file_name, substring, avg
)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from config import MINIO_BUCKET_TRUSTED, MINIO_BUCKET, MINIO_BUCKET_REFINED
from datetime import datetime
from sptrans_client import save_spark_df_to_temp_parquet
from previsao_schema import schema_trusted
from postgres_client import send_postgres, send_postgres_media_linhas, send_postgres_media_linhas_veiculos

# Função para rodar o job Spark
def process_task_media_linha_veiculos():
    # Inicializando a sessão Spark
    spark = (
        SparkSession.builder
        .appName("Projeto2025")
        .getOrCreate()
    )

    # Configurando o cliente MinIO
    client = get_minio_client()
    
    prefix = "Previsao/" 
    objects = client.list_objects(MINIO_BUCKET_REFINED, prefix=prefix, recursive=True)
    
    df_list = []
    for obj in objects:
        object_name = obj.object_name
        df = read_from_minio_parquet(MINIO_BUCKET_REFINED, object_name)
        df_list.append(df)    
    
    df_grouped = pd.concat(df_list, ignore_index=True)
    print(df_grouped)
    df = spark.createDataFrame(df_grouped, schema=schema_trusted)
    print(df)
    lista_cl = df.select("cl").distinct().rdd.flatMap(lambda x: x).collect()
    schema = StructType([
        StructField("linha", IntegerType(), True),
        StructField("veiculo", IntegerType(), True),
        StructField("media_diferenca", IntegerType(), True),
        StructField("media_minutos", StringType(), True)
    ])
        
        
    df_final = spark.createDataFrame([], schema)

    for cl in lista_cl:
        
        df_cl = df.filter(col("cl") == cl)
        
        lista = df_cl.select("p").distinct().rdd.flatMap(lambda x: x).collect()
        
        for item in lista:
            print(cl, item)
            
            df_filter = (
                df_cl.filter(col("p") == item)
                .dropDuplicates(["cp"])
                .select("hr","cp","p","np","lt0","lt1","t","a")
                .orderBy("t")
            )
            
            if df_filter.count() <= 1:
                pass
            else:
                window_spec = Window.orderBy("minutos")
                
                df_filter = (
                    df_filter.withColumn("minutos", expr("CAST(SPLIT(t, ':')[0] AS INT) * 60 + CAST(SPLIT(t, ':')[1] AS INT)"))
                    .withColumn("diff_minutes", col("minutos") - lag("minutos", 1).over(window_spec))
                    .filter(col("diff_minutes").isNotNull())
                )
                
                df_media = (
                    df_filter.agg({"diff_minutes": "avg"})
                    .withColumnRenamed("avg(diff_minutes)", "media_diferenca")
                    .withColumn("media_diferenca", round(col("media_diferenca"), 1))
                    .withColumn("media_minutos",
                        concat(
                            lpad(floor(col("media_diferenca") % 60).cast("string"), 2, "0"),
                            lit(":"),
                            lpad(floor((col("media_diferenca") * 60) % 60).cast("string"), 2, "0")
                
                            )
                        )
                    .withColumn("veiculo",lit(int(item)))
                    .withColumn("linha",lit(cl))
                    .select(
                        "linha",
                        "veiculo",
                        "media_diferenca",
                        "media_minutos"
                    )
                )
                
                
                df_final = df_final.union(df_media)
            

    parquet_path = save_spark_df_to_temp_parquet(df_final)
    send_postgres_media_linhas_veiculos(df_final)
    print(f'Import no postgres realizado table: medias_linhas_veiculos')
    upload_to_minio(f"medias_linhas_veiculos/medias_linhas_veiculos.parquet",parquet_path, bucket=MINIO_BUCKET_TRUSTED)
    #df_final.write.format("parquet").mode("overwrite").save("s3a://trusted/medias_linhas_veiculos/")
    print(df_final.show())
    print("Arquivo 'medias_linhas_veiculos' salvo com sucesso")


def process_task_media_linha():
    # Inicializando a sessão Spark
    spark = (
        SparkSession.builder
        .appName("Projeto2025")
        .getOrCreate()
    )

    # Configurando o cliente MinIO
    client = get_minio_client()
    
    prefix = "Previsao/" 
    objects = client.list_objects(MINIO_BUCKET_REFINED, prefix=prefix, recursive=True)
    
    df_list = []
    for obj in objects:
        object_name = obj.object_name
        df = read_from_minio_parquet(MINIO_BUCKET_REFINED, object_name)
        df_list.append(df)    
    
    df_grouped = pd.concat(df_list, ignore_index=True)
    print(df_grouped)
    df = spark.createDataFrame(df_grouped, schema=schema_trusted)
    print(df)
        
    lista_cl = df.select("cl").distinct().rdd.flatMap(lambda x: x).collect()

    schema = StructType([
        StructField("linha", IntegerType(), True),
        StructField("media_diferenca", IntegerType(), True),
        StructField("media_minutos", StringType(), True)
    ])
        
        
    df_final = spark.createDataFrame([], schema)

    for cl in lista_cl:
        
        df_filter = (
            df.filter(col("cl") == cl)
            .dropDuplicates(["cp"])
            .select("hr","cp","p","np","lt0","lt1","t","a")
            .orderBy("t")
        )
        
        if df_filter.count() <= 1:
            pass
        else:
            window_spec = Window.orderBy("minutos")
            
            df_filter = (
                df_filter.withColumn("minutos", expr("CAST(SPLIT(t, ':')[0] AS INT) * 60 + CAST(SPLIT(t, ':')[1] AS INT)"))
                .withColumn("diff_minutes", col("minutos") - lag("minutos", 1).over(window_spec))
                .filter(col("diff_minutes").isNotNull())
            )
            
            df_media = (
                df_filter.agg({"diff_minutes": "avg"})
                .withColumnRenamed("avg(diff_minutes)", "media_diferenca")
                .withColumn("media_diferenca", round(col("media_diferenca"), 1))
                .withColumn("media_minutos",
                    concat(
                        lpad(floor(col("media_diferenca") % 60).cast("string"), 2, "0"),
                        lit(":"),
                        lpad(floor((col("media_diferenca") * 60) % 60).cast("string"), 2, "0")
            
                        )
                    )
                .withColumn("linha",lit(cl))
                .select(
                    "linha",
                    "media_diferenca",
                    "media_minutos"
                )
            )
            
            
            df_final = df_final.union(df_media)
            
    print(df_final.show())

    parquet_path = save_spark_df_to_temp_parquet(df_final)
    send_postgres_media_linhas(df_final)
    print(f'Import no postgres realizado table: medias_linhas')
    upload_to_minio(f"medias_linhas/medias_linhas.parquet",parquet_path, bucket=MINIO_BUCKET_TRUSTED)
    print(df_final.show())
    print("Arquivo 'medias_linhas' salvo com sucesso")


def process_task_veiculos_horario():
    # Inicializando a sessão Spark
    spark = (
        SparkSession.builder
        .appName("Projeto2025")
        .getOrCreate()
    )

    # Configurando o cliente MinIO
    client = get_minio_client()
    
    prefix = "Previsao/" 
    objects = client.list_objects(MINIO_BUCKET_REFINED, prefix=prefix, recursive=True)
    
    df_list = []
    for obj in objects:
        object_name = obj.object_name
        df = read_from_minio_parquet(MINIO_BUCKET_REFINED, object_name)
        df_list.append(df)    
    
    df_grouped = pd.concat(df_list, ignore_index=True)
    print(df_grouped)
    df = spark.createDataFrame(df_grouped, schema=schema_trusted)
    print(df)
        
    lista_cl = df.select("cl").distinct().rdd.flatMap(lambda x: x).collect()
    schema = StructType([
        StructField("linha", IntegerType(), True),
        StructField("veiculo", IntegerType(), True),
        StructField("media_diferenca", IntegerType(), True),
        StructField("media_minutos", StringType(), True)
    ])
        
        
    df_final = spark.createDataFrame([], schema)

    for cl in lista_cl:
        
        df_cl = df.filter(col("cl") == cl)
        
        lista = df_cl.select("p").distinct().rdd.flatMap(lambda x: x).collect()
        
        for item in lista:
            print(cl, item)
            
            df_filter = (
                df_cl.filter(col("p") == item)
                .dropDuplicates(["cp"])
                .select("hr","cp","p","np","lt0","lt1","t","a")
                .orderBy("t")
            )
            
            if df_filter.count() <= 1:
                pass
            else:
                window_spec = Window.orderBy("minutos")
                
                df_filter = (
                    df_filter.withColumn("minutos", expr("CAST(SPLIT(t, ':')[0] AS INT) * 60 + CAST(SPLIT(t, ':')[1] AS INT)"))
                    .withColumn("diff_minutes", col("minutos") - lag("minutos", 1).over(window_spec))
                    .filter(col("diff_minutes").isNotNull())
                )
                
                df_media = (
                    df_filter.agg({"diff_minutes": "avg"})
                    .withColumnRenamed("avg(diff_minutes)", "media_diferenca")
                    .withColumn("media_diferenca", round(col("media_diferenca"), 1))
                    .withColumn("media_minutos",
                        concat(
                            lpad(floor(col("media_diferenca") % 60).cast("string"), 2, "0"),
                            lit(":"),
                            lpad(floor((col("media_diferenca") * 60) % 60).cast("string"), 2, "0")
                
                            )
                        )
                    .withColumn("veiculo",lit(int(item)))
                    .withColumn("linha",lit(cl))
                    .select(
                        "linha",
                        "veiculo",
                        "media_diferenca",
                        "media_minutos"
                    )
                )
                
                
                df_final = df_final.union(df_media)
            
    print(df_final.show())
    send_postgres(df_final)
    print(f'Import no postgres realizado table: medias_veiculos_horario')
    parquet_path = save_spark_df_to_temp_parquet(df_final)
    upload_to_minio(f"medias_veiculos_horario/medias_veiculos_horario.parquet",parquet_path, bucket=MINIO_BUCKET_TRUSTED)
    print(df_final.show())
    print("Arquivo 'medias_veiculos_horario' salvo com sucesso")








# DAG definition
with DAG(
    dag_id="dag_spark_trusted",
    start_date=datetime(2025, 2, 5),
    schedule_interval="@daily",  # Defina conforme a necessidade
    catchup=False,
) as dag:
    
    # Task 1: Processar Linhas, Paradas e Previsao
    process_task_media_linha_veiculos = PythonOperator(
        task_id="process_task_media_linha_veiculos",
        python_callable=process_task_media_linha_veiculos,
    )

       # Task 2: Processar Linhas, Paradas e Previsao
    process_task_media_linha = PythonOperator(
        task_id="process_task_media_linha",
        python_callable=process_task_media_linha,
    )

           # Task 2: Processar Linhas, Paradas e Previsao
    process_task_veiculos_horario = PythonOperator(
        task_id="process_task_veiculos_horario",
        python_callable=process_task_veiculos_horario,
    )

    process_task_media_linha_veiculos >> process_task_media_linha >> process_task_veiculos_horario
