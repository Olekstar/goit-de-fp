import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, current_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType

# Налаштування логування (для прикладу)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


#############################################
# Функція для збереження кожного мікробатчу
#############################################
def foreach_batch_function(batch_df, batch_id):
    """
    У кожному мікробатчі записуємо дані у:
    (a) Новий Kafka-топік (final_topic_oleksandr_topic)
    (b) Таблицю MySQL (athlete_enriched_agg)
    """

    row_count = batch_df.count()
    logger.info(f"=== Batch {batch_id} ===")
    logger.info(f"Отримано {row_count} рядків для запису.")

    if row_count == 0:
        return  # Якщо немає даних у мікробатчі, виходимо

    # (A) Запис у Kafka
    df_for_kafka = batch_df.select(
        to_json(struct([batch_df[x] for x in batch_df.columns])).alias("value")
    )

    df_for_kafka.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="admin" '
            'password="VawEzo1ikLtrA8Ug8THa";'
        ) \
        .option("topic", "final_topic_oleksandr_topic") \
        .save()

    logger.info(f"Записано {row_count} рядків у Kafka-топік 'final_topic_oleksandr_topic'.")

    # (B) Запис у MySQL
    #
    # Якщо таблиці athlete_aggregates ще немає,
    # рекомендується створити її вручну або використати mode("overwrite").
    # Тут приклад із 'append', що вимагає наявності таблиці.
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://217.61.57.46:3306/oleksandr_s") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "athlete_enriched_agg") \
        .option("user", "neo_data_admin") \
        .option("password", "Proyahaxuqithab9oplp") \
        .mode("append") \
        .save()

    logger.info(f"Записано {row_count} рядків у таблицю 'oleksandr_s.athlete_enriched_agg' (mode=append).")


#############################################
# Головна програма (Steps 1,2,4,5,6)
#############################################
def main():
    spark = SparkSession.builder \
        .appName("EndToEndStreamingPipeline") \
        .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .getOrCreate()

    # -----------------------
    # (1) Зчитати дані фізичних показників атлетів (athlete_bio)
    # -----------------------
    logger.info("Читаємо athlete_bio із MySQL...")

    df_bio = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://217.61.57.46:3306/olympic_dataset") \
        .option("dbtable", "athlete_bio") \
        .option("user", "neo_data_admin") \
        .option("password", "Proyahaxuqithab9oplp") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    logger.info(f"Зчитано {df_bio.count()} рядків із athlete_bio.")

    # -----------------------
    # (2) Відфільтрувати порожні / некоректні (height, weight)
    # -----------------------
    logger.info("Фільтруємо дані за height і weight...")

    df_bio_casted = df_bio \
        .withColumn("height", col("height").cast(FloatType())) \
        .withColumn("weight", col("weight").cast(FloatType()))

    df_bio_filtered = df_bio_casted.filter(
        (col("height").isNotNull()) & (col("weight").isNotNull())
    )

    logger.info(f"Після фільтрації лишилося {df_bio_filtered.count()} рядків.")

    # -----------------------
    # (4) Зчитуємо результати змагань із Kafka та об'єднуємо з біоданими
    # -----------------------
    # Схема JSON для athlete_event_results
    event_schema = StructType([
        StructField("edition", StringType()),
        StructField("edition_id", IntegerType()),
        StructField("country_noc", StringType()),
        StructField("sport", StringType()),
        StructField("event", StringType()),
        StructField("result_id", LongType()),
        StructField("athlete", StringType()),
        StructField("athlete_id", IntegerType()),
        StructField("pos", StringType()),
        StructField("medal", StringType()),
        StructField("isTeamSport", StringType())
    ])

    logger.info("Читаємо стрімінгово з Kafka-топіка athlete_event_results_oleksandr_topic...")

    df_kafka_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
        .option("subscribe", "athlete_event_results_oleksandr_topic") \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username=\"admin\" '
            'password=\"VawEzo1ikLtrA8Ug8THa\";'
        ) \
        .load()

    logger.info("Парсимо JSON у колонки...")

    df_event_parsed = df_kafka_raw \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*")

    logger.info("Join за athlete_id...")

    df_joined = df_event_parsed.join(
        df_bio_filtered,  # batch DataFrame
        on="athlete_id",
        how="inner"
    )

    # -----------------------
    # (5) Знайти середній зріст і вагу (groupBy)
    # -----------------------
    logger.info("Join за athlete_id...")

    df_joined = (
        df_event_parsed.alias("ev")
        .join(
            df_bio_filtered.alias("bio"),
            on="athlete_id",
            how="inner"
        )
        .select(
            col("ev.sport"),
            col("ev.medal"),
            col("ev.athlete_id"),
            col("bio.height"),
            col("bio.weight"),
            col("bio.sex"),
            col("bio.country_noc"),
        )
    )

    logger.info("Обчислюємо агрегацію...")

    df_aggregated = df_joined.groupBy(
        "sport",
        "medal",
        "sex",
        "country_noc"
    ).agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ).withColumn(
        "calculation_ts", current_timestamp()
    )

    # -----------------------
    # (6) Запис (forEachBatch) у новий Kafka-топік та MySQL
    # -----------------------
    logger.info("Налаштовуємо foreachBatch для запису результатів...")

    query = df_aggregated \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("complete") \
        .start()

    logger.info("Пайплайн запущено. Очікуємо нових даних у Kafka...")

    query.awaitTermination()


if __name__ == "__main__":
    main()
