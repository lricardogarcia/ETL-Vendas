from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
import os

# Caminho absoluto do diretório do projeto
BASE_DIR = os.path.abspath(os.path.dirname(__file__) + "/../")

# Adiciona o diretório do projeto ao sys.path para evitar erro de importação
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Agora podemos importar os módulos corretamente
from scripts.extract import extract
from scripts.transform import transform
from scripts.load import load

# Configuração padrão do DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 21),
    'retries': 1,
}

dag = DAG(
    'etl_vendas',
    default_args=default_args,
    description='Pipeline ETL de Vendas',
    schedule_interval='@daily',
)

# Tarefas do pipeline ETL
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load,
    dag=dag,
)

# Definir ordem de execução das tarefas
extract_task >> transform_task >> load_task
