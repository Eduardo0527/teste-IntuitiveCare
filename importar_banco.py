import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
import sys
import re

SENHA_MYSQL = "senha" #Colocar senha SQL nesse campo

senha_tratada = urllib.parse.quote_plus(SENHA_MYSQL)
if SENHA_MYSQL:
    db_connection_str = f'mysql+pymysql://root:{senha_tratada}@localhost/teste_ans'
else:
    db_connection_str = 'mysql+pymysql://root:@localhost/teste_ans'

def extrair_registro(texto):
    match = re.search(r'Reg\. ANS (\d+)', str(texto))
    if match: return match.group(1)
    return None

try:
    print(f"1. Conectando ao MySQL...")
    db_connection = create_engine(db_connection_str)
    
    with db_connection.connect() as connection:
        print("   -> Conexão OK!")

    print("\n2. Processando OPERADORAS (Cadastro)...")
    arquivo_cad = 'dados_enriquecidos_validados.csv'
    
    df_cadastro = pd.read_csv(arquivo_cad, sep=';', encoding='utf-8', dtype=str)
    
    df_cadastro.columns = [c.strip() for c in df_cadastro.columns]

    if 'RegistroANS' not in df_cadastro.columns:
        print("   [ERRO] Coluna RegistroANS não encontrada no arquivo de cadastro.")
        print(f"   Colunas disponíveis: {df_cadastro.columns.tolist()}")
        sys.exit()

    for col in ['CNPJ', 'RazaoSocial', 'Modalidade', 'UF']:
        if col not in df_cadastro.columns: df_cadastro[col] = None

    df_ops = df_cadastro[['RegistroANS', 'CNPJ', 'RazaoSocial', 'Modalidade', 'UF']].drop_duplicates(subset=['RegistroANS'])
    df_ops.columns = ['registro_ans', 'cnpj', 'razao_social', 'modalidade', 'uf']

    df_ops['uf'] = df_ops['uf'].replace({'N/D': None, 'nan': None, 'None': None})
    df_ops['uf'] = df_ops['uf'].astype(str).str.slice(0, 2)
    df_ops['uf'] = df_ops['uf'].replace({'No': None, 'na': None, 'None': None})

    print("   -> Inserindo Operadoras no Banco...")
    df_ops.to_sql('operadoras', con=db_connection, if_exists='append', index=False)


    print("\n3. Processando DESPESAS...")
    arquivo_desp = 'resultado_final.csv'
    
    df_despesas = pd.read_csv(arquivo_desp, sep=';', encoding='utf-8', dtype={'Trimestre': str, 'Ano': int})
    df_despesas.columns = [c.strip() for c in df_despesas.columns]
    
    if 'RegistroANS' not in df_despesas.columns:
        print("   -> Coluna RegistroANS não existe no CSV de despesas. Extraindo da Razão Social...")
        df_despesas['RegistroANS'] = df_despesas['RazaoSocial'].apply(extrair_registro)
        df_despesas = df_despesas.dropna(subset=['RegistroANS'])
        
    df_desp_sql = df_despesas[['RegistroANS', 'Trimestre', 'Ano', 'ValorDespesas']].copy()
    
    if df_desp_sql['ValorDespesas'].dtype == 'O':
        df_desp_sql['ValorDespesas'] = df_desp_sql['ValorDespesas'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    
    df_desp_sql['ValorDespesas'] = pd.to_numeric(df_desp_sql['ValorDespesas'])
    
    df_desp_sql.columns = ['registro_ans', 'trimestre', 'ano', 'valor_despesa']

    print("   -> Inserindo Despesas no Banco...")
    df_desp_sql.to_sql('despesas', con=db_connection, if_exists='append', index=False)

    print("\n--- SUCESSO! DADOS IMPORTADOS ---")

except Exception as e:
    print(f"\n[ERRO FATAL]: {e}")