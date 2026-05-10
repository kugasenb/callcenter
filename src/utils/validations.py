


class ValidationSchema:


    @staticmethod
    def valida_schema(df, schema):

        # CAPTURA OS CAMPOS DO ARQUIVO #
        campos_arquivo = set(df.columns)

        campos_schema = set([
            field.name
            for field in schema.fields
        ])

        if campos_arquivo == campos_schema:
            print("Schema válido")
        else:
            print("Schema inválido")

            print("Faltando:")
            print(campos_schema - campos_arquivo)

            print("Extras:")
            print(campos_arquivo - campos_schema)