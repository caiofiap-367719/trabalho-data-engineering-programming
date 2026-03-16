import logging

from pyspark.sql.functions import col, to_date, year
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType


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
                StructField("ID_PEDIDO", StringType(), True),
                StructField("PRODUTO", StringType(), True),
                StructField("VALOR_UNITARIO", DoubleType(), True),
                StructField("QUANTIDADE", IntegerType(), True),
                StructField("DATA_CRIACAO", StringType(), True),
                StructField("UF", StringType(), True),
                StructField("ID_CLIENTE", StringType(), True)
            ])

            pedidos = (
                self.spark.read
                .schema(pedidos_schema)
                .option("header", True)
                .option("sep", ";")
                .csv(pedidos_path)
            )


            pedidos = (
                pedidos
                .withColumn("valor_total", col("VALOR_UNITARIO") * col("QUANTIDADE"))
                .withColumn("data_pedido", to_date(col("DATA_CRIACAO"), "yyyy-MM-dd'T'HH:mm:ss"))
                .filter(year(col("data_pedido")) == 2025)
                .withColumnRenamed("ID_PEDIDO", "id_pedido")
                .withColumnRenamed("UF", "estado")
            )


            logging.info("Lendo dataset de pagamentos")

            pagamentos_schema = StructType([
                StructField("id_pedido", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("status", BooleanType(), True),
                StructField("fraude", BooleanType(), True)
            ])

            pagamentos = (
                self.spark.read
                .schema(pagamentos_schema)
                .json(pagamentos_path)
            )


            pagamentos_filtrados = pagamentos.filter(
                (col("status") == False) &
                (col("fraude") == False)
            )


            df_final = (
                pedidos.join(
                    pagamentos_filtrados,
                    "id_pedido",
                    "left"
                )
                .select(
                    "id_pedido",
                    "estado",
                    "forma_pagamento",
                    "valor_total",
                    "data_pedido"
                )
                .orderBy(
                    "estado",
                    "forma_pagamento",
                    "data_pedido"
                )
            )


            logging.info("Relatório gerado com sucesso")

            return df_final


        except Exception as e:

            logging.error(f"Erro na geração do relatório: {str(e)}")

            raise
