import uuid

from pyspark.sql import SparkSession

from quality.validators.validator import Validator
from quality.engine.engine import Engine

from quality.repositories.metadata_repository import MetadataRepository
from quality.repositories.event_repository import EventRepository
from quality.repositories.validation_repository import ValidationRepository

from pyspark.dbutils import DBUtils

from quality.pipelines.quality_discagem import PIPELINE_CONFIG


def quality_executor(path=None, process_name=None, debug=None, **kwargs):

    spark = SparkSession.getActiveSession()

    dbutils = DBUtils(spark)

    if process_name is None:

        process_name = PIPELINE_CONFIG["nm_pipeline"]

    if path is None:

        path = PIPELINE_CONFIG["path"]

    arquivos = dbutils.fs.ls(path)

    if debug:

        arquivos = arquivos[:10]

    for arquivo in arquivos:

        if not arquivo.path.endswith(".csv"):

            continue

        id_processamento = str(uuid.uuid4())

        print(f"Processando arquivo: {arquivo.name}")

        df = (
            spark.read
            .option("header", True)
            .option("sep", ";")
            .csv(arquivo.path)
        )

        resultados = Validator.run(
            df=df
            , rules=PIPELINE_CONFIG["rules"]
        )

        payload = Engine.processar_resultados(
            resultados=resultados
            , nm_arquivo=arquivo.name
            , path_arquivo=arquivo.path
            , nm_pipeline=process_name
            , id_processamento=id_processamento
        )

        ValidationRepository.salvar_validacoes(payload)

        MetadataRepository.salvar_status(payload)

        EventRepository.salvar_evento(payload)

        print(f"Finalizado: {arquivo.name}")