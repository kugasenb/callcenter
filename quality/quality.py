
from pyspark.sql import functions as f


class Quality:



    @staticmethod
    def validate_date_column(
                df,
                column_name,
                date_format="yyyy-MM-dd"
            ):
                """
                Valida uma coluna de data.

                A função:
                - verifica se a data é válida
                - verifica se o formato está correto
                - retorna flag booleana

                Args:
                    df:
                        DataFrame Spark.

                    column_name:
                        Nome da coluna de data.

                    date_format:
                        Formato esperado da data.

                Returns:
                    DataFrame:
                        DataFrame com coluna de validação.
                """

                validation_column = f"fl_{column_name}_valida"

                return df.withColumn(
                    validation_column,

                    f.when(
                        f.try_to_date(
                            f.col(column_name),
                            date_format
                        ).isNotNull(),

                        f.lit(1)

                    ).otherwise(
                        f.lit(0)
                    )
                )

    @staticmethod
    def validate_schema(
            df: DataFrame,
            schema: StructType
        ) -> bool:
            """
            Valida se os campos de um DataFrame
            correspondem ao schema esperado.

            A validação compara:
            - nomes dos campos
            - quantidade de campos

            Args:
                df (DataFrame):
                    DataFrame que será validado.

                schema (StructType):
                    Schema esperado para validação.

            Returns:
                bool:
                    True para schema válido.
                    False para schema inválido.
            """

            campos_arquivo = set(df.columns)

            campos_schema = set([
                field.name
                for field in schema.fields
            ])

            if campos_arquivo == campos_schema:

                print("Schema válido")

                return True

            else:

                print("Schema inválido")

                print("Campos faltantes:")
                print(campos_schema - campos_arquivo)

                print("Campos extras:")
                print(campos_arquivo - campos_schema)

                return False


    @staticmethod
    def consolidar_lista_df(lista_df):

        df_consolidado = lista_df[0]

        for df in lista_df[1:]:

            df_consolidado = df_consolidado.unionByName(df)

        return df_consolidado


    @staticmethod
    def validate_reference_column(
            df,
            df_reference,
            column_name,
            reference_column=None
        ):
            """
            Valida se os valores de uma coluna
            existem em uma tabela de referência.

            Args:
                df:
                    DataFrame principal.

                df_reference:
                    DataFrame de referência/homologação.

                column_name:
                    Coluna que será validada.

                reference_column:
                    Nome da coluna no DataFrame de referência.
                    Caso não informado, utiliza o mesmo nome.

            Returns:
                DataFrame:
                    DataFrame com flag de validação.
            """

            if reference_column is None:
                reference_column = column_name

            validation_column = f"fl_{column_name}_valida"

            df_reference = df_reference.select(
                f.col(reference_column)
            ).distinct()

            return (
                df.alias("main")
                .join(
                    df_reference.alias("ref"),

                    f.col(f"main.{column_name}")
                    ==
                    f.col(f"ref.{reference_column}"),

                    "left"
                )
                .withColumn(

                    validation_column,

                    f.when(
                        f.col(f"ref.{reference_column}").isNotNull(),
                        f.lit(1)
                    ).otherwise(
                        f.lit(0)
                    )
                )
                .drop(f"ref.{reference_column}")
            )