from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.getActiveSession()


class EventRepository:

    @staticmethod
    def salvar_evento(payload):

        rows = [{

            "id_processamento": payload["id_processamento"]
            , "nm_pipeline": payload["nm_pipeline"]
            , "nm_arquivo": payload["nm_arquivo"]
            , "path_arquivo": payload["path_arquivo"]
            , "evento": payload["evento"]
            , "status": payload["status"]
            , "mensagem": payload["mensagem"]

        }]

        df = (
            spark
            .createDataFrame(rows)
            .withColumn(
                "dt_evento"
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
                "quality_layer.quality.file_events"
            )
        )