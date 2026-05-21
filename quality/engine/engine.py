from datetime import datetime


class Engine:

    @staticmethod
    def processar_resultados(
        resultados,
        nm_arquivo
    ):

        # =====================================
        # AJUSTA OBJETOS COMPLEXOS
        # =====================================

        for r in resultados:

            if "amostra" in r:

                r["amostra"] = str(
                    r["amostra"]
                )

        # =====================================
        # TOTAL ERROS
        # =====================================

        total_erros = sum(
            r["total_erros"]
            for r in resultados
        )

        # =====================================
        # STATUS
        # =====================================

        status = (
            "rejeitado"
            if total_erros > 0
            else "aprovado"
        )

        # =====================================
        # PAYLOAD
        # =====================================

        payload = {

            "id_execucao": int(
                datetime.now().timestamp()
            ),

            "arquivo": nm_arquivo,

            "escritorio": "esc_001",

            "status": status,

            "total_linhas": 0,

            "total_erros": total_erros,

            "dt_execucao": datetime.now(),

            "resultados": resultados
        }

        return payload