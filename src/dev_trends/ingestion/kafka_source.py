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


def read_kafka_batch(
    spark: SparkSession,
    topic: str,
    bootstrap_servers: str,
    starting_offsets: str = "earliest",
    ending_offsets: str = "latest",
) -> DataFrame:
    """Lee un topic de Kafka como query BATCH acotada (no streaming).

    Usa spark.read (no readStream): Es la base del reproceso dirigido
    de una ventana ("reprocesar un día que llegó mal").

    Args:
        spark: SparkSession (debe incluir el conector spark-sql-kafka).
        topic: Topic a consumir.
        bootstrap_servers: Brokers de Kafka (e.g. 'localhost:9092').
        starting_offsets: Offset inicial. 'earliest' (defecto) o un JSON de
            offsets por partición. En batch NO se admite 'latest' aquí.
        ending_offsets: Offset final. 'latest' (defecto = hasta el final del log)
            o un JSON de offsets por partición.

    Returns:
        DataFrame batch con el esquema fuente de Kafka (key, value, topic,
        partition, offset, timestamp, ...).
    """
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", ending_offsets)
        .load()
    )
