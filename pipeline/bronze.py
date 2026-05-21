import pandas as pd
import time
from pyspark.sql import functions as f
from pyspark.sql import types as t
import random

from src.utils.validations import *
from src.utils.file import *

from datetime import datetime

from pyspark.sql import SparkSession

from zoneinfo import ZoneInfo

from pyspark.dbutils import DBUtils


spark = SparkSession.getActiveSession()

dbutils = DBUtils(spark)


def brz_disc_call(dir_path):

    data_hoje = datetime.now(
        ZoneInfo("America/Sao_Paulo")
    ).strftime("%Y%m%d")

    arquivos = dbutils.fs.ls(dir_path)

    arquivos_hoje = []

    for arq in arquivos:

        print(arq.path)

        if (

            arq.path.endswith(".csv")

            and

            f"discagem_{data_hoje}_" in arq.path

        ):

            arquivos_hoje.append(
                arq.path
            )

    print(arquivos_hoje)

    if not arquivos_hoje:

        raise Exception(
            f"Nenhum arquivo encontrado para {data_hoje}"
        )

    df_carga = FileReader.read_csv(
        path=arquivos_hoje,
        header=True,
        use_spark=True,
        sep=";"
    )

    return df_carga