class Pipeline:

    def __init__(self, sales_service, config):
        self.sales_service = sales_service
        self.config = config

    def run(self):
        df = self.sales_service.generate_report(
            self.config.pedidos_path,
            self.config.pagamentos_path
        )

        (
            df.write
            .mode("overwrite")
            .parquet(self.config.output_path)
        )