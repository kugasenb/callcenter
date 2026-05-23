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

CONFIG_PATH = f"file:/Workspace{repo_root}/dlt/quality/dlt_config.json"

# ============================================
# LOAD CONFIG
# ============================================

config_content = dbutils.fs.head(CONFIG_PATH, 100000)

CONFIG = json.loads(config_content)

# ============================================
# BUILD TABLES
# ============================================

for table in CONFIG["tables"]:
    module = importlib.import_module(table["transform"]["module"])

    transform_function = getattr(module, table["transform"]["fn"])

    def build_table(transform_function=transform_function, table=table):
        params = table.get("params", {})

        return transform_function(**params)

    dlt.table(name=table["name"])(build_table)
