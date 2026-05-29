from pyspark.sql import SparkSession


spark = SparkSession.getActiveSession()


class ValidationRepository:

    @staticmethod
    def salvar_validacoes(payload):

        rows = []

        for resultado in payload["resultados"]:

            rows.append({

                "nm_arquivo": payload["nm_arquivo"],
                "regra": resultado["regra"],
                "status": resultado["status"],
                "qtd_invalidos": resultado["qtd_invalidos"]

            })

        df = spark.createDataFrame(rows)

        (
            df.write
            .mode("append")
            .saveAsTable(
                "quality_layer.quality.validation_results"
            )
        )