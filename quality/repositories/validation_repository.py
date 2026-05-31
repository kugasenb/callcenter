from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.getActiveSession()


class ValidationRepository:

    @staticmethod
    def salvar_validacoes(payload):

        rows = []

        for resultado in payload["resultados"]:

            rows.append({

                "id_processamento": payload["id_processamento"]
                , "nm_pipeline": payload["nm_pipeline"]
                , "nm_arquivo": payload["nm_arquivo"]
                , "path_arquivo": payload["path_arquivo"]
                , "regra": resultado["regra"]
                , "status": resultado["status"]
                , "qtd_invalidos": resultado["qtd_invalidos"]

            })

        df = (
            spark
            .createDataFrame(rows)
            .withColumn(
                "dt_processamento"
                , f.from_utc_timestamp(
                    f.current_timestamp()
                    , "America/Sao_Paulo"
                )
            )
        )

        (
            df.write
            .mode("append")
            .saveAsTable(
                "quality_layer.quality.validation_results"
            )
        )