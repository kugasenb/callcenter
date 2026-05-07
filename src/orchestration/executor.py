import json
import importlib
import dlt
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CONFIG_PATH = "/Workspace/Users/lucasenb1@gmail.com/callcenter/dlt/dlt_config.json"


with open(CONFIG_PATH, "r") as file:
    CONFIG = json.load(file)


for table in CONFIG["tables"]:

    module = importlib.import_module(table["module"])

    transform_function = getattr(module, table["fn"])

    table_name = table["name"]

    path = table["path"]

    def create_table(fn=transform_function, path=path):
        return fn(spark, path)

    create_table.__name__ = table_name

    dlt.table(
        name=table_name
    )(create_table)