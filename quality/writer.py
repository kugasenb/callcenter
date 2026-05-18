from pyspark.sql import SparkSession
from datetime import datetime


spark = SparkSession.getActiveSession()


class Writer:

    @staticmethod
    def gravar_execucao(payload):

        data = [(
            payload["id_execucao"],
            payload["arquivo"],
            payload["escritorio"],
            payload["status"],
            payload["total_linhas"],
            payload["total_erros"],
            payload["dt_execucao"]
        )]

        df = spark.createDataFrame(
            data,
            [
                "id_execucao",
                "arquivo",
                "escritorio",
                "status_arquivo",
                "total_linhas",
                "total_erros",
                "dt_execucao"
            ]
        )

        (
            df.write
            .mode("append")
            .saveAsTable(
                "quality.execucao_arquivo"
            )
        )

    @staticmethod
    def gravar_erros_arquivo(payload):

        data = []

        for r in payload["resultados"]:

            if r["total_erros"] > 0:

                data.append((
                    payload["id_execucao"],
                    r["regra"],
                    "alta",
                    f'{r["campo"]} com {r["total_erros"]} erros',
                    datetime.now()
                ))

        if not data:
            return

        df = spark.createDataFrame(
            data,
            [
                "id_execucao",
                "regra",
                "severidade",
                "detalhe",
                "dt_erro"
            ]
        )

        (
            df.write
            .mode("append")
            .saveAsTable(
                "quality.erros_arquivo"
            )
        )

    @staticmethod
    def gravar_erros_linha(payload):

        data = []

        for r in payload["resultados"]:

            if r["total_erros"] > 0:

                amostra = r["amostra"].collect()

                for row in amostra:

                    data.append((
                        payload["id_execucao"],
                        row["linha_arquivo"]
                        if "linha_arquivo" in row
                        else None,

                        r["campo"],
                        r["regra"],

                        str(
                            row[r["campo"]]
                        ) if r["campo"] in row
                        else None,

                        datetime.now()
                    ))

        if not data:
            return

        df = spark.createDataFrame(
            data,
            [
                "id_execucao",
                "linha_arquivo",
                "campo",
                "tipo_erro",
                "valor_original",
                "dt_erro"
            ]
        )

        (
            df.write
            .mode("append")
            .saveAsTable(
                "quality.erros_linha"
            )
        )