from pyspark.sql import functions as f


class Rules:

    @staticmethod
    def validar_telefone_nulo(df):

        qtd_invalidos = (
            df
            .filter(
                f.col("Telefone").isNull()
            )
            .count()
        )

        return {
            "regra": "validar_telefone_nulo"
            , "status": "PASSED" if qtd_invalidos == 0 else "REJECTED"
            , "qtd_invalidos": qtd_invalidos
        }

    @staticmethod
    def validar_data_nula(df):

        qtd_invalidos = (
            df
            .filter(
                f.col("data_discagem").isNull()
            )
            .count()
        )

        return {
            "regra": "validar_data_nula"
            , "status": "PASSED" if qtd_invalidos == 0 else "REJECTED"
            , "qtd_invalidos": qtd_invalidos
        }