import pandas as pd
from sqlalchemy import create_engine
import configparser
from transform import transform

def load():
    # Ler configuração do banco de dados
    config = configparser.ConfigParser()
    config.read('/opt/airflow/config/database.ini')
    
    user = config['postgresql']['user']
    password = config['postgresql']['password']
    host = config['postgresql']['host']
    database = config['postgresql']['database']
    port = config['postgresql']['port']
    
    # Criar conexão
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    
    df = transform()
    
    df.to_sql('vendas', engine, if_exists='append', index=False)
    print("✅ Dados carregados no PostgreSQL!")

if __name__ == "__main__":
    load()
