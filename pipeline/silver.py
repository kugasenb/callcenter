from quality.quality import Quality

from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from zoneinfo import ZoneInfo
from datetime import datetime
import time
from pyspark.sql import SparkSession
from src.utils.file import Conversor


spark = SparkSession.getActiveSession()

def silver(df):
    campos_int = [
        "tempo_espera_s"
        , "tempo_atend_s"
    ]
    campos_float = [
        "valor_acordo"
    ]

    campos_data = [
        "data_discagem"
    ]


    df_int =  Conversor.convert_columns(df=df, columns=campos_int, target_type="int")


    return df_int