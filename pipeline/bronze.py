from quality.quality import Quality

from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from zoneinfo import ZoneInfo
from datetime import datetime
import time
from pyspark.sql import SparkSession

valida_schema = Quality.validate_schema
consolida_df = Quality.consolidar_lista_df
valida_referencia_coluna = Quality.validate_reference_column
validacao_data = Quality.validate_date_column

spark = SparkSession.getActiveSession()

def bronze(dir_path, df, df_escritorio, schema):

    dfs = []

    df_files = (
        spark.read
        .format("binaryFile")
        .load(f"{dir_path}/*")
    )

    arquivos = [
        row.path
        for row in df_files.select("path").collect()
    ]

    for file_path in arquivos:

        nome_arquivo = file_path.split("/")[-1]

        partes_nome = nome_arquivo.split("_")

        if len(partes_nome) < 2:
            continue

        data_arquivo = partes_nome[1]

        # TEMPORARIAMENTE SEM FILTRO
        if nome_arquivo == "discagem_20260510_esc_002.csv":
            print(nome_arquivo)
            df = spark.read.csv(file_path, header=True, sep=";")
            



    return df