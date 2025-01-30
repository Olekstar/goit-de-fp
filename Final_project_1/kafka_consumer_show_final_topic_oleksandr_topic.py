import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Налаштуємо логування
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("FinalTopicConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
        .getOrCreate()

    logger.info("Підключаємось до Kafka-топіка final_topic_oleksandr_topic ...")

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
        .option("subscribe", "final_topic_oleksandr_topic") \
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

    # Приводимо value до рядка
    df_strings = df_kafka.selectExpr("CAST(value AS STRING) AS message_string")

    # Вивід у консоль без обрізання (truncate=false), по 50 рядків за раз
    query = df_strings.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .option("numRows", 50) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
