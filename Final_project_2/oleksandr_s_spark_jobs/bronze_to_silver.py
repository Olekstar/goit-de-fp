"""
bronze_to_silver.py
Чистка тексту + дублі, запис у silver.
"""

import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def clean_text(text):
    # Забирає символи, крім (a-z, A-Z, 0-9, ',', '.', '\\', '"' тощо)
    return re.sub(r'[^a-zA-Z0-9,.\\\"\' ]', '', str(text))

def main(table_name):
    spark = SparkSession.builder \
        .appName(f"bronze_to_silver_{table_name}") \
        .getOrCreate()

    # 1. Читаємо parquet з bronze/<table_name>
    bronze_path = f"bronze/{table_name}"
    df = spark.read.parquet(bronze_path)

    # 2. Очищення тексту у текстових колонках
    clean_text_udf = udf(clean_text, StringType())
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, clean_text_udf(col(field.name)))

    # 3. Видаляємо дублі
    df = df.dropDuplicates()

    # >>> Показуємо перші 10 рядків перед записом
    df.show(10, truncate=False)

    # 4. Запис у silver/<table_name>
    silver_path = f"silver/{table_name}"
    df.write.mode("overwrite").parquet(silver_path)
    print(f"Written cleaned & deduplicated data to {silver_path}")

    spark.stop()

if __name__ == "__main__":
    main("athlete_bio")
    main("athlete_event_results")
