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
        from pyspark.sql import functions as f

        if use_spark:
            return (
                spark.read.options(**kwargs)
                .csv(path)
                .withColumn(
                    "source_file",
                    f.element_at(f.split(f.col("_metadata.file_path"), "/"), -1),
                )
            )

        return pd.read_csv(path, **kwargs)

    @staticmethod
    def read_excel(path: str, **kwargs) -> pd.DataFrame:
        """
        Realiza a leitura de um arquivo Excel.

        Args:
            path (str):
                Caminho completo do arquivo.

            **kwargs:
                Parâmetros adicionais do pandas.read_excel().

        Returns:
            pd.DataFrame:
                DataFrame contendo os dados do arquivo.
        """

        return pd.read_excel(path, **kwargs)

    @staticmethod
    def read_json(path: str, **kwargs) -> pd.DataFrame:
        """
        Realiza a leitura de um arquivo JSON.

        Args:
            path (str):
                Caminho completo do arquivo.

            **kwargs:
                Parâmetros adicionais do pandas.read_json().

        Returns:
            pd.DataFrame:
                DataFrame contendo os dados do arquivo.
        """

        return pd.read_json(path, **kwargs)

    @staticmethod
    def read_parquet(path: str, **kwargs) -> pd.DataFrame:
        """
        Realiza a leitura de um arquivo Parquet.

        Args:
            path (str):
                Caminho completo do arquivo.

            **kwargs:
                Parâmetros adicionais do pandas.read_parquet().

        Returns:
            pd.DataFrame:
                DataFrame contendo os dados do arquivo.
        """

        return pd.read_parquet(path, **kwargs)


class FileWriter:
    """
    Classe responsável pela escrita de DataFrames em arquivos.

    Suporta diferentes formatos de saída como:
    CSV, Excel, JSON e Parquet.

    Objetivo:
    Padronizar a exportação de dados nos pipelines
    e aplicações da arquitetura.
    """

    @staticmethod
    def write_csv(df: pd.DataFrame, path: str, **kwargs) -> None:
        """
        Salva um DataFrame em formato CSV.

        Args:
            df (pd.DataFrame):
                DataFrame que será exportado.

            path (str):
                Caminho completo do arquivo de saída.

            **kwargs:
                Parâmetros adicionais do pandas.to_csv().
        """

        df.to_csv(path, index=False, **kwargs)

    @staticmethod
    def write_excel(df: pd.DataFrame, path: str, **kwargs) -> None:
        """
        Salva um DataFrame em formato Excel.

        Args:
            df (pd.DataFrame):
                DataFrame que será exportado.

            path (str):
                Caminho completo do arquivo de saída.

            **kwargs:
                Parâmetros adicionais do pandas.to_excel().
        """

        df.to_excel(path, index=False, **kwargs)

    @staticmethod
    def write_json(df: pd.DataFrame, path: str, **kwargs) -> None:
        """
        Salva um DataFrame em formato JSON.

        Args:
            df (pd.DataFrame):
                DataFrame que será exportado.

            path (str):
                Caminho completo do arquivo de saída.

            **kwargs:
                Parâmetros adicionais do pandas.to_json().
        """

        df.to_json(path, **kwargs)

    @staticmethod
    def write_parquet(df: pd.DataFrame, path: str, **kwargs) -> None:
        """
        Salva um DataFrame em formato Parquet.

        Args:
            df (pd.DataFrame):
                DataFrame que será exportado.

            path (str):
                Caminho completo do arquivo de saída.

            **kwargs:
                Parâmetros adicionais do pandas.to_parquet().
        """

        df.to_parquet(path, index=False, **kwargs)


class Conversor:
    @staticmethod
    def convert_unix_time(file):
        timestamp = datetime.fromtimestamp(file.modificationTime / 1000)
        return timestamp

    @staticmethod
    def convert_columns(df, columns: list, target_type: str):
        for column in columns:
            df = df.withColumn(column, f.col(column).cast(target_type))

        return df
