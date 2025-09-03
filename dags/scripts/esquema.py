from sqlalchemy import create_engine, text
import logging

COMANDOS_SQL_CRIACAO = [
    """
    CREATE TABLE IF NOT EXISTS public.transacoes (
        cod_transacao INTEGER,
        num_conta BIGINT,
        data_transacao TIMESTAMP WITH TIME ZONE,
        nome_transacao VARCHAR(255),
        valor_transacao NUMERIC(15, 2)
    );
    """
]


def criar_tabelas_dw(db_uri: str):
    logging.info("Iniciando verificação do esquema no Data Warehouse.")
    engine = create_engine(db_uri)

    try:
        with engine.connect() as connection:
            # Inicia uma transação.
            trans = connection.begin()
            try:
                for comando in COMANDOS_SQL_CRIACAO:
                    connection.execute(text(comando))
                trans.commit()
                logging.info("Tabela 'transacoes' verificada/criada com sucesso!")
            except Exception as e:
                logging.error(f"Erro durante a execução do comando SQL. Revertendo. Erro: {e}")
                trans.rollback()
                raise
    except Exception as e:
        logging.error(f"Não foi possível conectar ao banco de dados: {e}")
        raise