from pyspark.sql import types as t

SCHEMA_QUALITY_DISCAGEM = t.StructType([

    t.StructField("arquivo", t.StringType(), True),
    t.StructField("caminho_arquivo", t.StringType(), True),

    t.StructField("total_linhas", t.IntegerType(), True),

    t.StructField("coluna", t.StringType(), True),
    t.StructField("regra", t.StringType(), True),

    t.StructField("total_erros", t.IntegerType(), True),
    t.StructField("percentual_erros", t.DoubleType(), True),

    t.StructField("status_arquivo", t.StringType(), True),

    t.StructField("dt_execucao", t.TimestampType(), True),

    t.StructField("pipeline", t.StringType(), True)

])