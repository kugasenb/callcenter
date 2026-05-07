

def build_bronze_discagem(spark, path):

    arquivo = r"/Volumes/workspace/callcenter/dumps/discagem_20260101_esc_001.csv"




    df = spark.read.csv(arquivo, header=True, inferSchema=True, sep=";")


    return df