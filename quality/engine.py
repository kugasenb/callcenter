
from datetime import datetime


class Engine:

    @staticmethod
    def processar_resultados(
        resultados,
        nm_arquivo
    ):

        total_erros = sum(
            r["total_erros"]
            for r in resultados
        )

        status = (
            "rejeitado"
            if total_erros > 0
            else "aprovado"
        )

        payload = {

            "arquivo": nm_arquivo,

            "status": status,

            "total_erros": total_erros,

            "dt_execucao": datetime.now(),

            "resultados": resultados
        }

        return payload