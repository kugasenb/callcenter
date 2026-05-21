from pyspark.sql import SparkSession

from quality.engine.rules import Rules
from quality.engine.engine import Engine
from quality.writers.writer import Writer


spark = SparkSession.getActiveSession()


class Quality:

    @staticmethod
    def validar_arquivo(df, nm_arquivo):

        resultados = [

            Rules.validar_telefone_nulo(df),
            Rules.validar_data(df)

        ]

        payload = Engine.processar_resultados(
            resultados=resultados,
            nm_arquivo=nm_arquivo
        )

        Writer.gravar_execucao(payload)

        Writer.gravar_erros_arquivo(payload)

        Writer.gravar_erros_linha(payload)

        return payload


# =========================================
# DLT WRAPPERS
# =========================================

def execucao_arquivo(dir_path):

    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .csv(dir_path)
    )

    payload = Quality.validar_arquivo(
        df=df,
        nm_arquivo=dir_path
    )

    return spark.createDataFrame([payload])


def erros_linha(dir_path):

    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .csv(dir_path)
    )

    payload = Quality.validar_arquivo(
        df=df,
        nm_arquivo=dir_path
    )

    return spark.createDataFrame(
        payload["resultados"]
    )


def erros_arquivo(dir_path):

    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .csv(dir_path)
    )

    payload = Quality.validar_arquivo(
        df=df,
        nm_arquivo=dir_path
    )

    return spark.createDataFrame(
        payload["resultados"]
    )