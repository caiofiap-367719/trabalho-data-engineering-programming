# Data Engineering Programming - Trabalho Final

## Objetivo
Este projeto tem como objetivo gerar um relatório de pedidos de venda com pagamentos recusados (`status = false`) e classificados como legítimos (`fraude = false`).

O relatório contém os seguintes campos:
- `id_pedido`
- `estado`
- `forma_pagamento`
- `valor_total`
- `data_pedido`

Considerando apenas pedidos do ano de 2025.

A saída é gravada em formato **Parquet**.

## Estrutura do projeto

```bash
config/
orchestrator/
src/
tests/
main.py
requirements.txt
pyproject.toml
MANIFEST.in
README.md