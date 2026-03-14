import logging

from pyspark.sql.functions import col, year, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class SalesService:

    def __init__(self, spark):

        self.spark = spark


    def generate_report(self, pedidos_path, pagamentos_path):

        try:

            logging.info("Lendo dataset de pedidos")

            pedidos_schema = StructType([
                StructField("id_pedido", StringType(), True),
                StructField("estado", StringType(), True),
                StructField("valor_total", DoubleType(), True),
                StructField("data_pedido", StringType(), True)
            ])

            pedidos = self.spark.read \
                .schema(pedidos_schema) \
                .option("header", True) \
                .csv(pedidos_path)


            logging.info("Lendo dataset de pagamentos")

            pagamentos_schema = StructType([
                StructField("id_pedido", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("status", BooleanType(), True),
                StructField("fraude", BooleanType(), True)
            ])

            pagamentos = self.spark.read \
                .schema(pagamentos_schema) \
                .json(pagamentos_path)


            pedidos = pedidos.withColumn(
                "data_pedido",
                to_date(col("data_pedido"))
            )


            pedidos_2025 = pedidos.filter(
                year(col("data_pedido")) == 2025
            )


            pagamentos_filtrados = pagamentos.filter(
                (col("status") == False) &
                (col("fraude") == False)
            )


            df_final = pedidos_2025.join(
                pagamentos_filtrados,
                "id_pedido"
            ).select(
                "id_pedido",
                "estado",
                "forma_pagamento",
                "valor_total",
                "data_pedido"
            ).orderBy(
                "estado",
                "forma_pagamento",
                "data_pedido"
            )


            logging.info("Relatório gerado com sucesso")

            return df_final


        except Exception as e:

            logging.error(f"Erro na geração do relatório: {str(e)}")

            raise