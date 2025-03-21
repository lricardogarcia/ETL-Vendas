import pandas as pd

def extract():
    print("🔄 Extraindo dados do CSV...")
    df = pd.read_csv('../data/vendas.csv')  # Caminho do CSV
    print(df.head())
    return df

if __name__ == "__main__":
    extract()
