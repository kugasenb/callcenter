from pyspark.sql import functions as f


class Rules:

    @staticmethod
    def validar_telefone_nulo(df):

        df_erro = df.filter(
            f.col("telefone").isNull()
        )

        return {
            "regra": "telefone_nulo",
            "campo": "telefone",
            "total_erros": df_erro.count(),
            "amostra": df_erro.limit(100)
        }

    @staticmethod
    def validar_data(df):

        df_erro = df.filter(
            f.try_to_timestamp(
                f.col("data_discagem"),
                f.lit("yyyy-MM-dd")
            ).isNull()
        )

        return {
            "regra": "data_invalida",
            "campo": "data_discagem",
            "total_erros": df_erro.count(),
            "amostra": df_erro.limit(100)
        }