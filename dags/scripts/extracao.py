import pandas as pd
from sqlalchemy import create_engine
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def criar_diretorio_destino(base_path: str, origem: str, data_execucao: str) -> str:
    caminho = os.path.join(base_path, data_execucao, origem)
    os.makedirs(caminho, exist_ok=True)
    logging.info(f"Diretório criado (se não existia): {caminho}")
    return caminho


def extrair_de_csv(caminho_origem: str, destino_base: str, nome_arquivo: str, data_execucao: str) -> str:
    logging.info(f"Lendo arquivo CSV: {caminho_origem}")

    try:
        dados = pd.read_csv(caminho_origem)

        pasta_destino = criar_diretorio_destino(destino_base, "csv", data_execucao)
        arquivo_destino = os.path.join(pasta_destino, nome_arquivo)

        dados.to_csv(arquivo_destino, index=False)
        logging.info(f"CSV salvo em: {arquivo_destino}")
        return arquivo_destino

    except Exception as erro:
        logging.error(f"Erro ao extrair CSV: {erro}")
        raise


def extrair_de_sql(uri_banco: str, tabela: str, destino_base: str, data_execucao: str) -> str:
    logging.info(f"Consultando dados da tabela: {tabela}")

    try:
        engine = create_engine(uri_banco)
        dados = pd.read_sql(f"SELECT * FROM {tabela}", engine)

        pasta_destino = criar_diretorio_destino(destino_base, "sql", data_execucao)
        arquivo_destino = os.path.join(pasta_destino, f"{tabela}.csv")

        dados.to_csv(arquivo_destino, index=False)
        logging.info(f"Tabela '{tabela}' salva em: {arquivo_destino}")
        return arquivo_destino

    except Exception as erro:
        logging.error(f"Erro ao extrair SQL ({tabela}): {erro}")
        raise
