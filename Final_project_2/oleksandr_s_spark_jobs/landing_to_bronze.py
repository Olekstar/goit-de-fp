"""
landing_to_bronze.py
Завантаження CSV з FTP + збереження в bronze.
"""

import os
import requests
import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

def download_data(table_name):
    """
    Завантажує CSV із заданого FTP у локальний файл table_name.csv
    з відображенням прогресу завантаження.
    """
    base_url = "https://ftp.goit.study/neoversity/"
    file_url = base_url + f"{table_name}.csv"
    logger.info(f"Downloading from {file_url}")

    # Робимо GET-запит у режимі stream=True, щоб читати по частинах
    response = requests.get(file_url, stream=True)

    if response.status_code == 200:
        # Створюємо папку 'landing' у поточній директорії (якщо її немає)
        os.makedirs("landing", exist_ok=True)

        # Шлях для збереження CSV
        local_file = f"landing/{table_name}.csv"

        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192  # розмір блоку (8 KB)
        downloaded = 0

        with open(local_file, "wb") as f:
            logger.info(f"File opened for writing: {local_file}")
            # Зчитуємо дані chunk'ами і виводимо прогрес
            for data in response.iter_content(block_size):
                f.write(data)
                downloaded += len(data)
                if total_size > 0:
                    done = int(50 * downloaded / total_size)
                    percent = (downloaded / total_size) * 100
                    print(f"\rDownloading: [{'=' * done}{' ' * (50 - done)}] {percent:.2f}%", end='')

        print()  # новий рядок після прогрес-бару
        logger.info(f"File downloaded successfully and saved as {local_file}")
    else:
        msg = f"Failed to download {table_name}. Status code: {response.status_code}"
        logger.error(msg)
        raise Exception(msg)

def main(table_name):
    """
    1) Download CSV into landing/
    2) Read CSV with Spark
    3) Write as Parquet into bronze/<table_name>
    """
    logger.info(f"Starting landing_to_bronze for table: {table_name}")

    # 1. Download
    download_data(table_name)

    # 2. Read CSV with Spark
    spark = SparkSession.builder \
        .appName(f"landing_to_bronze_{table_name}") \
        .getOrCreate()

    csv_path = f"landing/{table_name}.csv"
    logger.info(f"Reading CSV from {csv_path}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_path)

    row_count = df.count()
    logger.info(f"Read {row_count} rows from {csv_path}")
    logger.debug("Schema details:")
    df.printSchema()

    # 3. Write to Parquet: bronze/<table_name>/
    bronze_path = f"bronze/{table_name}"
    logger.info(f"Writing dataframe to Parquet at {bronze_path} (overwrite mode)")

    # Показуємо приклад 10 рядків перед записом
    df.show(10, truncate=False)

    df.write.mode("overwrite").parquet(bronze_path)

    logger.info(f"Written to {bronze_path} in Parquet format.")
    spark.stop()
    logger.info(f"Completed landing_to_bronze for table: {table_name}")

if __name__ == "__main__":
    # Для прикладу запустимо обидві таблиці
    main("athlete_bio")
    main("athlete_event_results")
