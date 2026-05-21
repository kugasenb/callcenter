from quality.engine.rules import Rules
from quality.engine.engine import Engine
from quality.writers.writer import Writer


class Quality:
    @staticmethod
    def validar_arquivo(df, nm_arquivo):
        # =========================
        # EXECUTA REGRAS
        # =========================

        resultados = [Rules.validar_telefone_nulo(df), Rules.validar_data(df)]

        # =========================
        # PROCESSA RESULTADOS
        # =========================

        payload = Engine.processar_resultados(
            resultados=resultados, nm_arquivo=nm_arquivo
        )

        # =========================
        # GRAVA QUALITY
        # =========================

        Writer.gravar_execucao(payload)

        Writer.gravar_erros_arquivo(payload)

        Writer.gravar_erros_linha(payload)

        # =========================
        # APROVA / REPROVA
        # =========================

        if payload["status"] == "rejeitado":
            return payload
