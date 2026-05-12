import json
import importlib
import dlt

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CONFIG_PATH = "/Workspace/Users/lucasenb1@gmail.com/callcenter/dlt/dlt_config.json"

with open(CONFIG_PATH, "r") as file:

    CONFIG = json.load(file)


for table in CONFIG["tables"]:

    module = importlib.import_module(
        table["transform"]["module"]
    )

    transform_function = getattr(
        module,
        table["transform"]["fn"]
    )

    catalog = table["catalog"]

    schema = table["schema"]

    full_table_name = (
        f"{catalog}.{schema}.{table['name']}"
    )

    path = table["params"]["dir_path"]

    def create_table(
        fn=transform_function,
        path=path
    ):

        return fn(
            dir_path=path,
            df=None,
            df_escritorio=None,
            schema=None
        )

    create_table.__name__ = table["name"]

    dlt.table(
        name=full_table_name
    )(create_table)