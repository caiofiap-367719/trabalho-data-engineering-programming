from pyspark.sql import SparkSession

from config.config_reader import ConfigReader
from orchestrator.pipeline import Pipeline


def main():

    config = ConfigReader()

    spark = SparkSession.builder \
        .appName(config.app_name) \
        .getOrCreate()

    pipeline = Pipeline(spark, config)

    pipeline.run()


if __name__ == "__main__":
    main()