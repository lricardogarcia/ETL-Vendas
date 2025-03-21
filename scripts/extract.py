import pandas as pd

def extract():
    print("🔄 Extraindo dados do CSV...")
    df = pd.read_csv('/opt/airflow/data/vendas.csv')  # Caminho no container
    print(df.head())
    return df

if __name__ == "__main__":
    extract()
