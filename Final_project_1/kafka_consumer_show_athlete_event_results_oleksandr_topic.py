import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def foreach_batch_function(batch_df, batch_id):
    """
    Ця функція буде викликана для кожного мікробатчу.
    Тут можна робити складніші дії:
      - логувати/шукати помилки/записувати в базу
      - виводити кілька прикладів рядків
    """
    count = batch_df.count()
    logger.info(f"=== Batch {batch_id} ===")
    logger.info(f"Отримано {count} повідомлень у цьому мікробатчі.")

    # Виведемо кілька прикладів:
    batch_df.show(5, truncate=False)


def main():
    # 1. Створюємо SparkSession
    spark = SparkSession.builder \
        .appName("KafkaConsumerWithLogging") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .getOrCreate()

    # 2. Параметри Kafka
    topic_name = "athlete_event_results_oleksandr_topic"
    kafka_bootstrap = "77.81.230.104:9092"
    kafka_security_protocol = "SASL_PLAINTEXT"
    kafka_sasl_mechanism = "PLAIN"
    kafka_username = "admin"
    kafka_password = "VawEzo1ikLtrA8Ug8THa"

    logger.info(f"Підключення до Kafka | topic={topic_name}, bootstrap={kafka_bootstrap}")

    # 3. Налаштовуємо читання з Kafka (streaming)
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", kafka_security_protocol) \
        .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
        .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username=\"{kafka_username}\" password=\"{kafka_password}\";'
    ) \
        .load()

    # df_kafka містить колонки:
    # key, value, topic, partition, offset, timestamp, timestampType
    # Але key/value за замовчуванням у двійковому форматі (binary).

    # 4. Перетворимо key/value у String, щоб побачити в логах
    df_parsed = df_kafka.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("key").cast("string").alias("key_str"),
        col("value").cast("string").alias("value_str")
    )

    # 5. Використовуємо foreachBatch для обробки мікробатчів
    query = df_parsed.writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("append") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
