from pyspark.sql import DataFrame, SparkSession


def read_kafka_stream(spark: SparkSession, topic: str, bootstrap_servers: str) -> DataFrame:
    """Abre un stream de lectura sobre un topic de Kafka.

    Args:
        spark: SparkSession (debe incluir el conector spark-sql-kafka).
        topic: Topic a consumir.
        bootstrap_servers: Brokers de Kafka (e.g. 'localhost:9092').

    Returns:
        DataFrame de streaming con el esquema fuente de Kafka (key, value,
        topic, partition, offset, timestamp, ...).
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
