from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Configurando o Spark para se conectar ao MinIO
spark = (
    SparkSession.builder
    .appName("Projeto2025")
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# Exemplo de DataFrame do Spark e gravando em MinIO
data = [("João", 30), ("Maria", 25)]
columns = ["Nome", "Idade"]
df = spark.createDataFrame(data, columns)

# Salvando em MinIO como um arquivo Parquet
df.write.format("parquet").mode("overwrite").save("s3a://trusted/teste/data.parquet")

# Lendo o mesmo arquivo de volta
df2 = spark.read.format("parquet").load("s3a://trusted/teste/data.parquet")
df2.show()