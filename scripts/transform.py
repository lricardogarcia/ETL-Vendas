import pandas as pd
from extract import extract

def transform():
    df = extract()
    
    # Limpeza dos dados
    df['data_venda'] = pd.to_datetime(df['data_venda'])
    df['total'] = df['quantidade'] * df['preco_unitario']
    
    print("✅ Dados transformados com sucesso!")
    print(df.head())
    
    return df

if __name__ == "__main__":
    transform()
