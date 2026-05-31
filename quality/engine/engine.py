class Engine:

    @staticmethod
    def processar_resultados(
        resultados
        , nm_arquivo
        , path_arquivo
        , nm_pipeline
        , id_processamento
    ):

        status_final = "PASSED"

        for resultado in resultados:

            if resultado["status"] == "REJECTED":

                status_final = "REJECTED"

                break

        payload = {

            "id_processamento": id_processamento
            , "nm_pipeline": nm_pipeline
            , "nm_arquivo": nm_arquivo
            , "path_arquivo": path_arquivo
            , "status": status_final
            , "evento": "VALIDATION_FINISHED"
            , "mensagem": (
                f"Processamento finalizado "
                f"com status {status_final}"
            )
            , "resultados": resultados

        }

        return payload