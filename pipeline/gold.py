from quality.quality import Quality

from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from zoneinfo import ZoneInfo
from datetime import datetime
import time
from pyspark.sql import SparkSession

# =========================================
# GOLD - DISCAGEM DIA
# =========================================

def gld_discagem_dia(df):

    return (
        df
        .groupBy("data_discagem")
        .agg(
            f.count("id_chamada").alias("tot_discagens"),
            f.countDistinct("id_contrato").alias("tot_contratos"),
            f.sum("valor_acordo").alias("vlr_total_acordo"),
            f.avg("tempo_atend_s").alias("avg_tempo_atendimento")
        )
    )


# =========================================
# GOLD - COLABORADOR
# =========================================

def gld_colaborador(df):

    return (
        df
        .groupBy("id_colaborador")
        .agg(
            f.count("id_chamada").alias("tot_discagens"),
            f.sum("valor_acordo").alias("vlr_total_acordo"),
            f.avg("tempo_atend_s").alias("avg_tempo_atendimento")
        )
    )


# =========================================
# GOLD - FILA
# =========================================

def gld_fila(df):

    return (
        df
        .groupBy("fila")
        .agg(
            f.count("id_chamada").alias("tot_discagens"),
            f.sum("valor_acordo").alias("vlr_total_acordo")
        )
    )


# =========================================
# GOLD - FINALIZACAO
# =========================================

def gld_finalizacao(df):

    return (
        df
        .groupBy("finalizacao")
        .agg(
            f.count("id_chamada").alias("tot_discagens"),
            f.sum("valor_acordo").alias("vlr_total_acordo")
        )
    )


# =========================================
# GOLD - HORA
# =========================================

def gld_hora(df):

    return (
        df
        .withColumn(
            "hora",
            f.hour("hora_inicio")
        )
        .groupBy("hora")
        .agg(
            f.count("id_chamada").alias("tot_discagens"),
            f.sum("valor_acordo").alias("vlr_total_acordo")
        )
    )