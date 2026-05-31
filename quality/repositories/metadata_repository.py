from pyspark.sql import SparkSession
from delta.tables import DeltaTable


spark = SparkSession.getActiveSession()


class MetadataRepository:

    @staticmethod
    def salvar_status(payload):

        rows = [{

            "nm_arquivo": payload["nm_arquivo"]
            , "status": payload["status"]

        }]

        df = spark.createDataFrame(rows)

        delta_table = DeltaTable.forName(
            spark,
            "quality_layer.quality.file_control"
        )

        (
            delta_table.alias("target")
            .merge(
                df.alias("source")
                , "target.nm_arquivo = source.nm_arquivo"
            )

            .whenMatchedUpdate(set={

                "status": "source.status"
                , "dt_status": "from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')"

            })

            .whenNotMatchedInsert(values={

                "nm_arquivo": "source.nm_arquivo"
                , "status": "source.status"
                , "dt_status": "from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')"

            })

            .execute()
        ) 