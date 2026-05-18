from dataclasses import dataclass
from datetime import datetime


@dataclass
class ErroArquivo:

    regra: str
    severidade: str
    detalhe: str


@dataclass
class ErroLinha:

    linha_arquivo: int
    campo: str
    tipo_erro: str
    valor_original: str


@dataclass
class ExecucaoArquivo:

    id_execucao: int

    arquivo: str

    status_arquivo: str

    total_linhas: int
    total_erros: int

    dt_execucao: datetime