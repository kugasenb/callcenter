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

def bronze(df, dir_path, schema):
    
    
    df = spark.read.csv(dir_path, header=True, sep=";")
    df_fl_fin = df.withColumn(
        "fl_bronze",
        f.lit("campo_bronze")
    )
                



    return df_fl_fin