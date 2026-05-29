from pyspark.sql import SparkSession


spark = SparkSession.getActiveSession()


class EventRepository:

    @staticmethod
    def salvar_evento(payload):

        rows = [{

            "nm_arquivo": payload["nm_arquivo"],
            "status": payload["status"]

        }]

        df = spark.createDataFrame(rows)

        (
            df.write
            .mode("append")
            .saveAsTable(
                "quality_layer.quality.file_events"
            )
        )