# writer.py

from pyspark.sql import SparkSession


spark = SparkSession.getActiveSession()


class Writer:

    @staticmethod
    def gravar_execucao(payload):

        data = [(
            payload["arquivo"],
            payload["status"],
            payload["total_erros"],
            payload["dt_execucao"]
        )]

        df = spark.createDataFrame(
            data,
            [
                "arquivo",
                "status_arquivo",
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
                    r["regra"],
                    r["campo"],
                    r["total_erros"]
                ))

        if not data:
            return

        df = spark.createDataFrame(
            data,
            [
                "regra",
                "campo",
                "total_erros"
            ]
        )

        (
            df.write
            .mode("append")
            .saveAsTable(
                "quality.erros_arquivo"
            )
        )