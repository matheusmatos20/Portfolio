import pandas as pd
from sqlalchemy import create_engine
import psycopg2

def send_postgres_media_linhas(spark_df):
    try:
        # Converte o DataFrame Spark para Pandas
        df = spark_df.toPandas()

        # Conexão com o banco de dados PostgreSQL
        conn = psycopg2.connect(
            dbname="mydb",
            user="admin",
            password="admin123",
            host="host.docker.internal",
            port="5432"
        )
        cursor = conn.cursor()

        # Criar a tabela 'media_linhas' caso não exista
        create_table_query = """
            CREATE TABLE IF NOT EXISTS public.media_linhas (
                linha INT,
                media_diferenca FLOAT,
                media_minutos VARCHAR(5)
            );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # SQL de inserção para a tabela 'media_linhas'
        insert_query = """
            INSERT INTO public.media_linhas (linha, media_diferenca, media_minutos)
            VALUES (%s, %s, %s)
        """

        # Iterar sobre as linhas do DataFrame e inserir
        for _, row in df.iterrows():
            data = (row['linha'], row['media_diferenca'], row['media_minutos'])
            cursor.execute(insert_query, data)

        # Confirmando a transação
        conn.commit()

        print(f"{len(df)} linhas inseridas com sucesso!")

    except Exception as e:
        print(f"Erro ao inserir os dados: {e}")
    
    finally:
        # Fechar a conexão
        cursor.close()
        conn.close()


def send_postgres(spark_df):
    try:
        df = spark_df.toPandas()

        # Conexão com o banco de dados PostgreSQL
        conn = psycopg2.connect(
            dbname="mydb",
            user="admin",
            password="admin123",
            host="host.docker.internal",
            port="5432"
        )
        cursor = conn.cursor()

        # Criar a tabela caso não exista
        create_table_query = """
            CREATE TABLE IF NOT EXISTS public.media_veiculo_horario (
                linha INT,
                veiculo INT,
                media_diferenca FLOAT,
                media_minutos VARCHAR(5)
            );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # SQL de inserção
        insert_query = """
            INSERT INTO public.media_veiculo_horario (linha, veiculo, media_diferenca, media_minutos)
            VALUES (%s, %s, %s, %s)
        """

        # Iterar sobre as linhas do DataFrame e inserir
        for _, row in df.iterrows():
            data = (row['linha'], row['veiculo'], row['media_diferenca'], row['media_minutos'])
            cursor.execute(insert_query, data)

        # Confirmando a transação
        conn.commit()

        print(f"{len(df)} linhas inseridas com sucesso!")

    except Exception as e:
        print(f"Erro ao inserir os dados: {e}")
    
    finally:
        # Fechar a conexão
        cursor.close()
        conn.close()


def send_postgres_media_linhas_veiculos(spark_df):
    try:
        # Converte o DataFrame Spark para Pandas
        df = spark_df.toPandas()

        # Conexão com o banco de dados PostgreSQL
        conn = psycopg2.connect(
            dbname="mydb",
            user="admin",
            password="admin123",
            host="host.docker.internal",
            port="5432"
        )
        cursor = conn.cursor()

        # Criar a tabela 'media_linhas_veiculos' caso não exista
        create_table_query = """
            CREATE TABLE IF NOT EXISTS public.media_linhas_veiculos (
                linha INT,
                veiculo INT,
                media_diferenca FLOAT,
                media_minutos VARCHAR(5)
            );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # SQL de inserção para a tabela 'media_linhas_veiculos'
        insert_query = """
            INSERT INTO public.media_linhas_veiculos (linha, veiculo, media_diferenca, media_minutos)
            VALUES (%s, %s, %s, %s)
        """

        # Iterar sobre as linhas do DataFrame e inserir
        for _, row in df.iterrows():
            data = (row['linha'], row['veiculo'], row['media_diferenca'], row['media_minutos'])
            cursor.execute(insert_query, data)

        # Confirmando a transação
        conn.commit()

        print(f"{len(df)} linhas inseridas com sucesso!")

    except Exception as e:
        print(f"Erro ao inserir os dados: {e}")
    
    finally:
        # Fechar a conexão
        cursor.close()
        conn.close()

    