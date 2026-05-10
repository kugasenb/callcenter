
from callcenter.quality.quality import Quality
from pyspark.sql import functions as f
import time
from pyspark.sql import SparkSession



valida_schema = Quality.valida_schema
consolida_df = Quality.consolidar_lista_df
valida_referencia_coluna = Quality.validate_reference_column
validacao_data = Quality.validate_date_column

def bronze(dir_path, df, df_escritorio, schema):
        
    data_atual =  time.strftime("%Y%m%d")
    dfs = []

    for file in dbutils.fs.ls(dir_path):

        nome_arquivo = file.name

        # CAPTURA A DATA DO ARQUIVO #
        data_arquivo = nome_arquivo.split("_")[1]

        if data_arquivo == data_atual:
        

            file_path = f"{dir_path}/{nome_arquivo}"

            # LEITURA INICIAL DO ARQUIVO #
            df_process = spark.read.csv(file_path, sep=";", header=True)


            status_validation = valida_schema(df=df_process, schema=schema)

            df_sf = df_process.withColumn(
                "source_file",
                f.lit(file_path)
            )


            if status_validation:
                dfs.append(df_sf)
            else:
                print("Schema inválido")
                print("Arquivo: ", file_path)

            df_united = consolida_df(dfs)

            df = validacao_data(df_united, column_name="data_discagem", date_format="yyyy-MM-dd")

            df_flag_fin = valida_referencia_coluna(df, df_escritorio, column_name="id_escritorio")

            return df_flag_fin