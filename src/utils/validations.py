


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

    @staticmethod
    def valida_arquivos(data_atual: str = None):
        """
        data_atual formato: YYYYMMDD
        exemplo: 20260514
        """

        if data_atual is None:
            data_atual = datetime.now().strftime("%Y%m%d")

        arquivos_hoje = [
            file.name
            for file in dbutils.fs.ls(
                "/Volumes/workspace/callcenter/disc_volumes/landing/"
            )
            if data_atual in file.name
        ]

        for file in arquivos_hoje:
            print(file)
