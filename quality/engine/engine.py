from datetime import datetime


class Engine:
    @staticmethod
    def processar_resultados(resultados, nm_arquivo):
        total_erros = sum(r["total_erros"] for r in resultados)

        status = "rejeitado" if total_erros > 0 else "aprovado"

        payload = {
            "id_execucao": int(datetime.now().timestamp()),
            "arquivo": nm_arquivo,
            "escritorio": "esc_001",
            "status": status,
            "total_linhas": 0,
            "total_erros": total_erros,
            "dt_execucao": datetime.now(),
            "resultados": resultados,
        }

        return payload
