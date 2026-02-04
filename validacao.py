import pandas as pd
import requests
import io
import zipfile
import os
import re
import warnings
from bs4 import BeautifulSoup

# --- CONFIGURAÇÕES ---
INPUT_CSV_PART1 = "resultado_final.csv"
CADASTRO_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
OUTPUT_ENRICHED = "dados_enriquecidos_validados.csv"
OUTPUT_AGGREGATED = "relatorio_agregado.csv"

warnings.filterwarnings("ignore")

# --- FUNÇÕES AUXILIARES ---

def validar_cnpj(cnpj):
    """Valida CNPJ (Algoritmo oficial da Receita Federal)."""
    cnpj = re.sub(r'[^0-9]', '', str(cnpj))
    if len(cnpj) != 14 or len(set(cnpj)) == 1: return False
    
    # Validação do 1º Dígito
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma1 = sum(int(a) * b for a, b in zip(cnpj[:12], pesos1))
    resto1 = soma1 % 11
    digito1 = 0 if resto1 < 2 else 11 - resto1
    if int(cnpj[12]) != digito1: return False

    # Validação do 2º Dígito
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma2 = sum(int(a) * b for a, b in zip(cnpj[:13], pesos2))
    resto2 = soma2 % 11
    digito2 = 0 if resto2 < 2 else 11 - resto2
    if int(cnpj[13]) != digito2: return False
    return True

def baixar_dados_cadastrais():
    """Baixa CSV ou ZIP de operadoras ativas."""
    print("Acessando repositório de dados cadastrais...")
    try:
        response = requests.get(CADASTRO_URL, verify=False, timeout=30)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # 1. Tentar CSV direto
        csv_links = [l.get('href') for l in soup.find_all('a') if l.get('href', '').lower().endswith('.csv')]
        csv_links.sort(reverse=True)
        if csv_links:
            url_full = CADASTRO_URL + csv_links[0]
            print(f"Baixando CSV direto: {csv_links[0]}...")
            r = requests.get(url_full, verify=False)
            return pd.read_csv(io.BytesIO(r.content), sep=';', encoding='latin1', dtype=str, on_bad_lines='skip')
        
        # 2. Fallback ZIP
        zip_links = [l.get('href') for l in soup.find_all('a') if l.get('href', '').lower().endswith('.zip')]
        if zip_links:
            url_zip = CADASTRO_URL + zip_links[0]
            print(f"Baixando ZIP: {zip_links[0]}...")
            r = requests.get(url_zip, verify=False)
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                for f in z.namelist():
                    if f.lower().endswith('.csv'):
                        return pd.read_csv(z.open(f), sep=';', encoding='latin1', dtype=str)
        return pd.DataFrame()
    except Exception as e:
        print(f"Erro no download cadastral: {e}")
        return pd.DataFrame()

# --- EXECUÇÃO PRINCIPAL ---

print("=== PARTE 2: ENRIQUECIMENTO E VALIDAÇÃO ===")

if not os.path.exists(INPUT_CSV_PART1):
    print(f"ERRO: {INPUT_CSV_PART1} não encontrado.")
    exit()

print(f"1. Lendo arquivo de despesas: {INPUT_CSV_PART1}")
df_desp = pd.read_csv(INPUT_CSV_PART1, sep=';', encoding='utf-8')

# --- Extração do Registro ANS da coluna RazaoSocial ---
def extrair_registro(texto):
    match = re.search(r'Reg\. ANS (\d+)', str(texto))
    if match: return match.group(1)
    return None

df_desp['RegistroANS'] = df_desp['RazaoSocial'].apply(extrair_registro)
df_desp = df_desp.dropna(subset=['RegistroANS']) # Remove quem não achou código

print(f"Extração concluída. Registros válidos para processamento: {len(df_desp)}")

# 2. Baixar Cadastro
df_cad = baixar_dados_cadastrais()
if df_cad.empty: 
    print("ERRO CRÍTICO: Não foi possível baixar o cadastro.")
    exit()

# Normalizar colunas do cadastro para MAIÚSCULAS
df_cad.columns = [c.upper().strip() for c in df_cad.columns]

# --- CORREÇÃO AQUI: Lista atualizada com REGISTRO_OPERADORA ---
coluna_registro_encontrada = None
possiveis_nomes = ['REGISTRO_OPERADORA', 'REG_ANS', 'CD_OPERADORA', 'REGISTRO', 'REGISTRO_ANS']

for col in possiveis_nomes:
    if col in df_cad.columns:
        coluna_registro_encontrada = col
        break

if not coluna_registro_encontrada:
    print("\nERRO: Não encontrei a coluna de Registro ANS no arquivo baixado.")
    print("Colunas disponíveis no arquivo baixado:", df_cad.columns.tolist())
    exit()

print(f"Coluna de Registro identificada como: '{coluna_registro_encontrada}'")

# Renomeia para o padrão 'RegistroANS' para o merge
df_cad.rename(columns={coluna_registro_encontrada: 'RegistroANS'}, inplace=True)

# Mapeamento do resto das colunas
mapa_cad = {
    'CNPJ': 'CNPJ_Real',
    'RAZAO_SOCIAL': 'RazaoSocial_Real',
    'MODALIDADE': 'Modalidade',
    'UF': 'UF'
}
# Só renomeia o que existe
cols_exist = {k: v for k, v in mapa_cad.items() if k in df_cad.columns}
df_cad.rename(columns=cols_exist, inplace=True)

# Seleciona apenas as colunas úteis (se existirem) + RegistroANS
colunas_finais_cad = ['RegistroANS'] + list(cols_exist.values())
df_cad_clean = df_cad[colunas_finais_cad].copy()

# Limpeza CNPJ Cadastro
if 'CNPJ_Real' in df_cad_clean.columns:
    df_cad_clean['CNPJ_Real'] = df_cad_clean['CNPJ_Real'].str.replace(r'[^0-9]', '', regex=True)

# 3. Join
print("\n2. Realizando cruzamento (JOIN) pelo RegistroANS...")

# Garante que a chave de join é string nas duas pontas e sem espaços
df_desp['RegistroANS'] = df_desp['RegistroANS'].astype(str).str.strip()
df_cad_clean['RegistroANS'] = df_cad_clean['RegistroANS'].astype(str).str.strip()

try:
    df_merged = pd.merge(df_desp, df_cad_clean, on='RegistroANS', how='left')
except Exception as e:
    print(f"Erro no Merge: {e}")
    exit()

# Preenchimento
if 'CNPJ_Real' in df_merged.columns:
    df_merged['CNPJ'] = df_merged['CNPJ_Real'].fillna('N/D')
else:
    df_merged['CNPJ'] = 'N/D'

if 'RazaoSocial_Real' in df_merged.columns:
    # Prioriza o nome oficial do cadastro, se não tiver, usa o do arquivo de despesas
    df_merged['RazaoSocial'] = df_merged['RazaoSocial_Real'].fillna(df_merged['RazaoSocial'])

# Modalidade e UF
if 'Modalidade' not in df_merged.columns: df_merged['Modalidade'] = 'N/D'
else: df_merged['Modalidade'] = df_merged['Modalidade'].fillna('N/D')

if 'UF' not in df_merged.columns: df_merged['UF'] = 'N/D'
else: df_merged['UF'] = df_merged['UF'].fillna('N/D')

# Limpeza final das colunas auxiliares
cols_drop = ['CNPJ_Real', 'RazaoSocial_Real']
df_merged.drop(columns=[c for c in cols_drop if c in df_merged.columns], inplace=True)

# --- VALIDAÇÃO ---
print("\n3. Validando dados...")
df_merged['ValorDespesas'] = pd.to_numeric(df_merged['ValorDespesas'], errors='coerce').fillna(0)
df_merged['CNPJ_Valido'] = df_merged['CNPJ'].apply(validar_cnpj)

print(f"Total Registros Processados: {len(df_merged)}")
print(f"CNPJs Inválidos ou Sem Match: {len(df_merged[~df_merged['CNPJ_Valido']])}")

df_merged.to_csv(OUTPUT_ENRICHED, index=False, sep=';', encoding='utf-8-sig')

# --- AGREGAÇÃO ---
print("\n4. Gerando relatório agregado...")
agg_params = {'ValorDespesas': 'sum'}

# Agrupa
cols_group = ['RazaoSocial', 'UF', 'CNPJ']
# Garante que colunas existem antes de agrupar
cols_group = [c for c in cols_group if c in df_merged.columns]

df_agg = df_merged.groupby(cols_group).agg(agg_params).reset_index()
df_agg.rename(columns={'ValorDespesas': 'DespesaTotal'}, inplace=True)

# Estatísticas Trimestrais (se possível)
if 'Trimestre' in df_merged.columns and 'Ano' in df_merged.columns:
    df_trim = df_merged.groupby(['RazaoSocial', 'UF', 'Trimestre'])['ValorDespesas'].sum().reset_index()
    df_stats = df_trim.groupby(['RazaoSocial', 'UF'])['ValorDespesas'].agg(['mean', 'std']).reset_index()
    df_stats.rename(columns={'mean': 'MediaTrimestral', 'std': 'DesvioPadraoTrimestral'}, inplace=True)
    
    df_final = pd.merge(df_agg, df_stats, on=['RazaoSocial', 'UF'], how='left')
    df_final['DesvioPadraoTrimestral'] = df_final['DesvioPadraoTrimestral'].fillna(0)
else:
    df_final = df_agg

# Formatação Numérica
cols_num = ['DespesaTotal', 'MediaTrimestral', 'DesvioPadraoTrimestral']
for c in cols_num:
    if c in df_final.columns: df_final[c] = df_final[c].round(2)

df_final.sort_values('DespesaTotal', ascending=False, inplace=True)
df_final.to_csv(OUTPUT_AGGREGATED, index=False, sep=';', encoding='utf-8-sig')

print(f"SUCESSO! Relatório final salvo em: {OUTPUT_AGGREGATED}")
print(df_final.head())