import pandas as pd
from sqlalchemy import create_engine, text
import io
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def carregar_dw(source_file_path: str, table_name: str, db_uri: str) -> None:

    logging.info(f"Iniciando carregamento de '{source_file_path}' para a tabela '{table_name}'.")

    try:
        df = pd.read_csv(source_file_path)
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo CSV: {e}")
        raise

    if df.empty:
        logging.warning("Arquivo CSV está vazio. Nada será carregado.")
        return


    engine = create_engine(db_uri)
    connection = engine.connect()
    transaction = connection.begin()

    try:
        logging.info(f"Limpando tabela de destino: {table_name}")
        connection.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"))

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep=",")
        buffer.seek(0)

        raw_connection = connection.connection
        cursor = raw_connection.cursor()

        copy_sql = f"COPY {table_name} ({','.join(df.columns)}) FROM STDIN WITH CSV"
        cursor.copy_expert(copy_sql, buffer)

        transaction.commit()
        logging.info(f"{len(df)} linhas carregadas com sucesso na tabela {table_name}.")

    except Exception as e:
        logging.error(f"Erro no carregamento. Executando rollback. Erro: {e}")
        transaction.rollback()
        raise
    finally:
        connection.close()
        engine.dispose()
