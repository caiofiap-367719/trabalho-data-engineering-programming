from pyspark.sql import SparkSession

from config.config_reader import ConfigReader
from orchestrator.pipeline import Pipeline
from src.sales import SalesService


def main():
    config = ConfigReader()

    spark = (
        SparkSession.builder
        .appName(config.app_name)
        .getOrCreate()
    )

    sales_service = SalesService(spark)
    pipeline = Pipeline(sales_service, config)

    pipeline.run()

    spark.stop()


if __name__ == "__main__":
    main()