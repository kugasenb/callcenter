from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


spark = SparkSession.getActiveSession()

dbutils = DBUtils(spark)


class FileManager:

    BASE_PATH = "dbfs:/Volumes/workspace/callcenter/disc_volumes"

    @staticmethod
    def move_file(source_path: str, target_folder: str):

        target_path = (
            f"{FileManager.BASE_PATH}/"
            f"{target_folder}/"
            f"{source_path.split('/')[-1]}"
        )

        dbutils.fs.mv(
            source_path,
            target_path
        )

        return target_path

    @staticmethod
    def move_to_processed(source_path: str):

        return FileManager.move_file(
            source_path=source_path,
            target_folder="processed"
        )

    @staticmethod
    def move_to_rejected(source_path: str):

        return FileManager.move_file(
            source_path=source_path,
            target_folder="rejected"
        )

    @staticmethod
    def move_to_approved(source_path: str):

        return FileManager.move_file(
            source_path=source_path,
            target_folder="approved"
        )