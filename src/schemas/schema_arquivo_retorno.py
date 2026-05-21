from pyspark.sql import types as t


SCHEMA_BRONZE_DISCAGEM = t.StructType(
    [
        # CAMPOS ORIGINAIS DO ARQUIVO #
        t.StructField("id_chamada", t.StringType(), True),
        t.StructField("id_contrato", t.StringType(), True),
        t.StructField("telefone", t.StringType(), True),
        t.StructField("id_colaborador", t.StringType(), True),
        t.StructField("id_escritorio", t.StringType(), True),
        t.StructField("fila", t.StringType(), True),
        t.StructField("data_discagem", t.StringType(), True),
        t.StructField("hora_inicio", t.StringType(), True),
        t.StructField("hora_fim", t.StringType(), True),
        t.StructField("tempo_espera_s", t.StringType(), True),
        t.StructField("tempo_atend_s", t.StringType(), True),
        t.StructField("finalizacao", t.StringType(), True),
        t.StructField("valor_acordo", t.StringType(), True),
        # METADADOS DE PROCESSAMENTO #
        t.StructField("nm_arquivo", t.StringType(), True),
        t.StructField("ds_caminho_arquivo", t.StringType(), True),
        t.StructField("dt_ingestao", t.TimestampType(), True),
        t.StructField("dt_processamento", t.TimestampType(), True),
        t.StructField("id_lote", t.StringType(), True),
    ]
)
