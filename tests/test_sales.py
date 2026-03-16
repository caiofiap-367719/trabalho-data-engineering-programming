from pyspark.sql import SparkSession
from src.sales import SalesService


def test_sales_service_creation():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )

    service = SalesService(spark)

    assert service is not None

    spark.stop()