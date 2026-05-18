from pyspark.sql import SparkSession
from pathlib import Path
import shutil


spark = SparkSession.getActiveSession()


class FileManager:

    BASE_PATH = "/Volumes/workspace/callcenter/disc_volumes"

    @staticmethod
    def move_file(source_path: str, target_folder: str):

        source = Path(source_path)

        target_dir = Path(
            f"{FileManager.BASE_PATH}/{target_folder}"
        )

        target_dir.mkdir(parents=True, exist_ok=True)

        target_path = target_dir / source.name

        shutil.move(
            str(source),
            str(target_path)
        )

        return str(target_path)

    @staticmethod
    def move_to_processing(source_path: str):

        return FileManager.move_file(
            source_path=source_path,
            target_folder="processing"
        )

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