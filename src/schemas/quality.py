from pyspark.sql import types as t


SCHEMA_QUALITY = t.StructType(
    [
        t.StructField("id_execucao", t.LongType(), True),
        t.StructField("linha_arquivo", t.LongType(), True),
        t.StructField("campo", t.StringType(), True),
        t.StructField("tipo_erro", t.StringType(), True),
        t.StructField("valor_original", t.StringType(), True),
        t.StructField("dt_erro", t.TimestampType(), True),
    ]
)
