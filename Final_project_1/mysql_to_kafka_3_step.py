import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct

# Налаштуємо логування
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    # 1. Створюємо SparkSession
    spark = SparkSession.builder \
        .appName("MySQLEventResultsToKafka") \
        .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .getOrCreate()

    logger.info("Зчитуємо дані з MySQL (таблиця athlete_event_results)...")

    # Зчитуємо дані з MySQL
    df_event_results = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://217.61.57.46:3306/olympic_dataset") \
        .option("dbtable", "athlete_event_results") \
        .option("user", "neo_data_admin") \
        .option("password", "Proyahaxuqithab9oplp") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    # Логуватимемо кількість рядків, які зчитали
    row_count = df_event_results.count()
    logger.info(f"З MySQL зчитано {row_count} рядків.")

    # Перевіримо приклад перших 5
    logger.info("Приклад 5 рядків:")
    df_event_results.show(5, truncate=False)

    # 2. Кожен рядок перетворюємо в JSON
    logger.info("Перетворюємо рядки у JSON...")
    df_to_kafka = df_event_results.select(
        to_json(struct([df_event_results[x] for x in df_event_results.columns])).alias("value")
    )

    # Перевіримо приклад 3 рядків JSON
    logger.info("Приклад JSON перед відправкою у Kafka:")
    df_to_kafka.show(3, truncate=False)

    # 3. Запис у Kafka (назва топіка - athlete_event_results)
    kafka_servers = "77.81.230.104:9092"
    topic_name = "athlete_event_results_oleksandr_topic"

    logger.info("Починаємо запис у Kafka...")
    logger.info(f"Kafka broker: {kafka_servers}, Topic: {topic_name}")

    df_to_kafka \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '\
            'username="admin" '\
            'password="VawEzo1ikLtrA8Ug8THa";'
        ) \
        .option("topic", topic_name) \
        .save()

    logger.info("Відправлення у Kafka успішно завершено!")

    spark.stop()

if __name__ == "__main__":
    main()
