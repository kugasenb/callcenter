from pyspark.sql import SparkSession

from quality.validators.validator import Validator
from quality.engine.engine import Engine

from quality.repositories.metadata_repository import MetadataRepository
from quality.repositories.event_repository import EventRepository
from quality.repositories.validation_repository import ValidationRepository
from pyspark.dbutils import DBUtils
from quality.pipelines.quality_discagem import PIPELINE_CONFIG


spark = SparkSession.getActiveSession()


arquivos = dbutils.fs.ls(
    PIPELINE_CONFIG["path"]
)


for arquivo in arquivos:

    if not arquivo.path.endswith(".csv"):
        continue

    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .csv(arquivo.path)
    )

    resultados = Validator.run(
        df=df,
        rules=PIPELINE_CONFIG["rules"]
    )

    payload = Engine.processar_resultados(
        resultados=resultados,
        nm_arquivo=arquivo.name
    )

    ValidationRepository.salvar_validacoes(payload)

    MetadataRepository.salvar_status(payload)

    EventRepository.salvar_evento(payload)