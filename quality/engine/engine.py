class Engine:

    @staticmethod
    def processar_resultados(resultados, nm_arquivo):

        status_final = "PASSED"

        for resultado in resultados:

            if resultado["status"] == "REJECTED":

                status_final = "REJECTED"

                break

        payload = {
            "nm_arquivo": nm_arquivo
            , "status": status_final
            , "resultados": resultados
        }

        return payload