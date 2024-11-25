import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("ctc_telligence_forecasting_data_product_test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.broadcastTimeout", "3600")
        .config("spark.sql.session.timeZone", "Europe/London")
    ).getOrCreate()
    return builder