import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


spark = SparkSession.builder.appName("").getOrCreate()


class FileReader:
    """
    Classe responsável pela leitura de arquivos.

    Suporta diferentes formatos de entrada como:
    CSV, Excel, JSON e Parquet.

    Objetivo:
    Centralizar e padronizar a leitura de arquivos
    utilizados nos pipelines de engenharia de dados.
    """

    @staticmethod
    def read_csv(path: str, use_spark=False, **kwargs):

        try:

            if use_spark:

                df = (
                    spark.read
                    .options(**kwargs)
                    .csv(path)
                    .withColumn(
                        "source_file",
                        f.element_at(
                            f.split(
                                f.col("_metadata.file_path"),
                                "/"
                            ),
                            -1
                        )
                    )
                )

                # força validação
                df.limit(1).collect()

            else:

                df = pd.read_csv(path, **kwargs)

                df["source_file"] = os.path.basename(path)

            return df

        except Exception as e:

            raise Exception(e)
