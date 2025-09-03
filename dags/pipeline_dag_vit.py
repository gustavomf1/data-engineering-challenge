from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os

from scripts.extracao import extrair_de_csv, extrair_de_sql
from scripts.envio import carregar_dw
from scripts.esquema import criar_tabelas_dw

DW_USER = os.getenv("USER_POSTGRE")
DW_PASSWORD = os.getenv("PASSWORD_POSTGRE")
DW_DB = os.getenv("DB_POSTGRE")
DW_HOST = "db"
DW_CONN_URI = f"postgresql+psycopg2://{DW_USER}:{DW_PASSWORD}@{DW_HOST}:5432/{DW_DB}"

CSV_FILE_PATH = "/opt/airflow/dados/transacoes.csv"
OUTPUT_DIR = "/opt/airflow/output"

TABELAS_SQL = [
    "agencias",
    "clientes",
    "colaboradores",
    "colaborador_agencia",
    "contas",
    "propostas_credito"
]

default_args = {
    'start_date': datetime(2025, 8, 1),
}

with DAG(
        dag_id="pipeline_banvic_etl_full",
        description="Pipeline ETL que extrai dados de CSV e de TODAS as tabelas do banco relacional.",
        schedule_interval="35 4 * * *",
        catchup=False,
        default_args=default_args,
        tags=["banvic", "etl", "full"],
        max_active_runs=1
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    extrair_csv = PythonOperator(
        task_id="extrair_transacoes_csv",
        python_callable=extrair_de_csv,
        op_kwargs={
            "caminho_origem": CSV_FILE_PATH,
            "destino_base": OUTPUT_DIR,
            "nome_arquivo": "transacoes.csv",
            "data_execucao": "{{ ds }}"
        }
    )

    tarefas_extracao_sql = []
    tarefas_carregamento_sql = []

    for tabela in TABELAS_SQL:
        task_extracao = PythonOperator(
            task_id=f"extrair_sql_{tabela}",
            python_callable=extrair_de_sql,
            op_kwargs={
                "uri_banco": DW_CONN_URI,
                "tabela": tabela,
                "destino_base": OUTPUT_DIR,
                "data_execucao": "{{ ds }}"
            }
        )
        tarefas_extracao_sql.append(task_extracao)

        task_carregamento = PythonOperator(
            task_id=f"carregar_{tabela}_dw",
            python_callable=carregar_dw,
            op_kwargs={
                "source_file_path": f"{{{{ ti.xcom_pull(task_ids='extrair_sql_{tabela}') }}}}",
                "table_name": tabela,
                "db_uri": DW_CONN_URI
            }
        )
        tarefas_carregamento_sql.append(task_carregamento)

    extracao_ok = EmptyOperator(task_id="extracao_finalizada")

    criar_esquema_dw = PythonOperator(
        task_id="criar_esquema_dw",
        python_callable=criar_tabelas_dw,
        op_kwargs={"db_uri": DW_CONN_URI}
    )

    carregar_csv = PythonOperator(
        task_id="carregar_transacoes_dw",
        python_callable=carregar_dw,
        op_kwargs={
            "source_file_path": "{{ ti.xcom_pull(task_ids='extrair_transacoes_csv') }}",
            "table_name": "transacoes",
            "db_uri": DW_CONN_URI
        }
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> [extrair_csv] + tarefas_extracao_sql >> extracao_ok

    extracao_ok >> criar_esquema_dw

    todas_as_cargas = [carregar_csv] + tarefas_carregamento_sql

    criar_esquema_dw >> todas_as_cargas[0]

    for i in range(len(todas_as_cargas) - 1):
        todas_as_cargas[i] >> todas_as_cargas[i + 1]

    todas_as_cargas[-1] >> fim
