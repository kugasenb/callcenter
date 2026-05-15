import pandas as pd
import time
from pyspark.sql import functions as f
from pyspark.sql import types as t
import random
from src.utils.validations import *
from src.utils.file import *
from datetime import datetime
from pyspark.sql import SparkSession


spark = SparkSession.getActiveSession()

def brz_disc_call(dir_path):
    data_hoje = datetime.now().strftime("%Y%m%d")

    path_hoje = (
    f"{dir_path}"
    f"discagem_{data_hoje}_*.csv"
    )
    
    df_carga = FileReader.read_csv(path=path_hoje, header=True, use_spark=True, sep=";")
    
    return df_carga