from pyspark.sql import DataFrame


def build_bronze_discagem(spark, path) -> DataFrame:

    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .csv(path)
    )

    return df