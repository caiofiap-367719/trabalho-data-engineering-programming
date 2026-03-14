from src.sales import SalesService


class Pipeline:

    def __init__(self, spark, config):

        self.spark = spark
        self.config = config


    def run(self):

        service = SalesService(self.spark)

        df = service.generate_report(
            self.config.pedidos_path,
            self.config.pagamentos_path
        )

        df.write.mode("overwrite").parquet(
            self.config.output_path
        )