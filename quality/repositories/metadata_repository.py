from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from delta.tables import DeltaTable


spark = SparkSession.getActiveSession()


class MetadataRepository:

    @staticmethod
    def salvar_status(payload):

        rows = [{

            "id_ultimo_processamento": payload["id_processamento"]
            , "nm_pipeline": payload["nm_pipeline"]
            , "nm_arquivo": payload["nm_arquivo"]
            , "path_arquivo": payload["path_arquivo"]
            , "status_atual": payload["status"]

        }]

        df = spark.createDataFrame(rows)

        tabela = "quality_layer.quality.file_control"

        tabela_existe = spark.catalog.tableExists(tabela)

        if not tabela_existe:

            df = df.withColumn(

                "dt_ultimo_processamento"

                , f.from_utc_timestamp(
                    f.current_timestamp()
                    , "America/Sao_Paulo"
                )

            )

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(tabela)
            )

            return

        delta_table = DeltaTable.forName(
            spark
            , tabela
        )

        (
            delta_table.alias("target")

            .merge(
                df.alias("source")
                , "target.path_arquivo = source.path_arquivo"
            )

            .whenMatchedUpdate(set={

                "id_ultimo_processamento": "source.id_ultimo_processamento"
                , "nm_pipeline": "source.nm_pipeline"
                , "nm_arquivo": "source.nm_arquivo"
                , "path_arquivo": "source.path_arquivo"
                , "status_atual": "source.status_atual"

                , "dt_ultimo_processamento": (
                    "from_utc_timestamp("
                    "current_timestamp(), "
                    "'America/Sao_Paulo'"
                    ")"
                )

            })

            .whenNotMatchedInsert(values={

                "id_ultimo_processamento": "source.id_ultimo_processamento"
                , "nm_pipeline": "source.nm_pipeline"
                , "nm_arquivo": "source.nm_arquivo"
                , "path_arquivo": "source.path_arquivo"
                , "status_atual": "source.status_atual"

                , "dt_ultimo_processamento": (
                    "from_utc_timestamp("
                    "current_timestamp(), "
                    "'America/Sao_Paulo'"
                    ")"
                )

            })

            .execute()
        )