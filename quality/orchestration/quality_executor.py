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

repo_root = "/".join(
    workspace_path.split("/")[:4]
)

# ============================================
# LOAD CONFIG
# ============================================

CONFIG_PATH = (
    f"/Workspace{repo_root}/dlt/quality/dlt_config.json"
)

with open(CONFIG_PATH, "r") as file:

    CONFIG = json.load(file)

# ============================================
# BUILD TABLES
# ============================================

for table in CONFIG["tables"]:

    module = importlib.import_module(
        table["transform"]["module"]
    )

    # ========================================
    # CLASS OR FUNCTION
    # ========================================

    if "class" in table["transform"]:

        cls = getattr(
            module,
            table["transform"]["class"]
        )

        transform_function = getattr(
            cls,
            table["transform"]["fn"]
        )

    else:

        transform_function = getattr(
            module,
            table["transform"]["fn"]
        )

    # ========================================
    # BUILD TABLE
    # ========================================

    def build_table(
        transform_function=transform_function,
        table=table
    ):

        params = table.get("params", {})

        return transform_function(**params)

    # ========================================
    # DLT TABLE
    # ========================================

    dlt.table(
        name=table["name"]
    )(build_table)