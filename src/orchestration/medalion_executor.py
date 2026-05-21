import json
import importlib
import dlt

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

# ============================================
# PATH CONFIG
# ============================================

workspace_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

repo_root = "/".join(workspace_path.split("/")[:4])

CONFIG_PATH = f"/Workspace{repo_root}/dlt/medalion/dlt_config.json"

# ============================================
# LOAD CONFIG
# ============================================

with open(CONFIG_PATH, "r") as file:
    CONFIG = json.load(file)

# ============================================
# BUILD TABLES
# ============================================

for table in CONFIG["tables"]:
    # ========================================
    # IGNORE METADATA
    # ========================================

    if "name" not in table:
        continue

    # ========================================
    # IMPORT MODULE
    # ========================================

    module = importlib.import_module(table["transform"]["module"])

    transform_function = getattr(module, table["transform"]["fn"])

    # ========================================
    # BUILD TABLE
    # ========================================

    def build_table(transform_function=transform_function, table=table):
        # ====================================
        # SOURCES
        # ====================================

        sources = {}

        for source in table["sources"]:
            alias = source["alias"]
            source_table = source["table"]

            sources[alias] = dlt.read(source_table.split(".")[-1])

        # ====================================
        # PARAMS
        # ====================================

        params = table.get("params", {})

        # ====================================
        # EXECUTE
        # ====================================

        return transform_function(**sources, **params)

    # ========================================
    # DLT TABLE
    # ========================================

    dlt.table(name=table["name"])(build_table)
