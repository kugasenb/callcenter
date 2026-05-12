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

    params = table.get(
        "params",
        {}
    )

    sources = table.get(
        "sources",
        []
    )

    def create_table(
        fn=transform_function,
        params=params,
        sources=sources
    ):

        kwargs = {}

        for source in sources:

            alias = source["alias"]

            source_table = source["table"]

            kwargs[alias] = dlt.read(
                source_table
            )

        return fn(
            **kwargs,
            **params
        )

    create_table.__name__ = table["name"]

    dlt.table(
        name=full_table_name
    )(create_table)