from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType

schema = StructType([
    StructField("hr", StringType(), True),
    StructField("p", StructType([  # "p" é um struct, não um array
        StructField("cp", IntegerType(), True),
        StructField("np", StringType(), True),
        StructField("px", DoubleType(), True),
        StructField("py", DoubleType(), True),
        StructField("l", ArrayType(  # "l" é um array de structs
            StructType([
                StructField("c", StringType(), True),
                StructField("cl", IntegerType(), True),
                StructField("sl", IntegerType(), True),
                StructField("lt0", StringType(), True),
                StructField("lt1", StringType(), True),
                StructField("qv", IntegerType(), True),
                StructField("vs", ArrayType(  # "vs" é um array de structs dentro de "l"
                    StructType([
                        StructField("p", StringType(), True),
                        StructField("t", StringType(), True),
                        StructField("a", BooleanType(), True),
                        StructField("ta", StringType(), True),
                        StructField("py", DoubleType(), True),
                        StructField("px", DoubleType(), True),
                        StructField("sv", StringType(), True),
                        StructField("is", StringType(), True),
                    ])
                ), True)
            ])
        ), True)
    ]), True)
])



schema_trusted = StructType([
    StructField("hr", StringType(), True),
    StructField("cp", IntegerType(), True),
    StructField("np", StringType(), True),
    StructField("px", DoubleType(), True),
    StructField("py", DoubleType(), True),
    StructField("c", StringType(), True),
    StructField("cl", IntegerType(), True),
    StructField("sl", IntegerType(), True),
    StructField("lt0", StringType(), True),
    StructField("lt1", StringType(), True),
    StructField("qv", IntegerType(), True),
    StructField("p", StringType(), True), 
    StructField("t", StringType(), True),
    StructField("a", BooleanType(), True),
    StructField("ta", StringType(), True),
    StructField("py_vs", DoubleType(), True),
    StructField("px_vs", DoubleType(), True),
    StructField("sv", StringType(), True),
    StructField("is", StringType(), True)
])
