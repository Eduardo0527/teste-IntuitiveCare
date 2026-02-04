import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import zipfile
import io
import os
import warnings

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/"
OUTPUT_CSV = "resultado_final.csv"

warnings.filterwarnings("ignore")

def gerar_soup(url):
    try:
        response = requests.get(url, timeout=30, verify=False)
        if response.status_code == 200:
            return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
    return None

def encontrar_demonstracoes(base_url):
    soup = gerar_soup(base_url)
    if not soup: return None
    
    for link in soup.find_all('a'):
        href = link.get('href', '')
        if 'demonstracoes' in href.lower() and 'contabeis' in href.lower():
            return base_url + href
    return None

def ultimos_trimestres(url, num=3):
    trimestres = []
    soup_anos = gerar_soup(url)
    anos = []
    if soup_anos:
        for link in soup_anos.find_all('a'):
            href = link.get('href', '')
            if re.match(r'^\d{4}/?$', href):
                anos.append(href.strip('/'))
    anos.sort(reverse=True)
    
    for ano in anos:
        ano_url = f"{url}{ano}/"
        soup_q = gerar_soup(ano_url)
        if not soup_q: continue
        
        trimestre_links = []
        for link in soup_q.find_all('a'):
            href = link.get('href','')
            if any(t in href.upper() for t in ['1T', '2T', '3T', '4T']):
                trimestre_links.append(href)
        trimestre_links.sort(reverse=True)
        
        for trimestre in trimestre_links:
            trimestres.append({
                'ano': ano,
                'trimestre': trimestre.strip('/'),
                'url': f"{ano_url}{trimestre}"
            })
            if len(trimestres) >= num:
                return trimestres
            
def processar_trimestre(t_dados):
    print(f"\n Processando: {t_dados['ano']} - {t_dados['trimestre']}")
    dfs = []
    urls_para_baixar = []
    
    
    if t_dados['url'].lower().endswith('.zip'):
        print("  Detectado link direto para arquivo ZIP.")
        urls_para_baixar.append(t_dados['url'])
    
    for zip_url in urls_para_baixar:
        print(f"  Baixando: {zip_url}...")
        
        try:
            r = requests.get(zip_url, verify=False)
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                for nomearquivo in z.namelist():
                    if nomearquivo.lower().endswith(('.csv', '.txt')):
                        with z.open(nomearquivo) as f:
                            try:
                                df = pd.read_csv(f, sep=';', encoding='latin1', on_bad_lines='skip')
                                df.columns = [c.upper().strip() for c in df.columns]
                                
                                
                                if 'CD_CONTA_CONTABIL' in df.columns and 'VL_SALDO_FINAL' in df.columns:
                                    
                                   
                                    df['CD_CONTA_CONTABIL'] = df['CD_CONTA_CONTABIL'].astype(str)
                                    filtro = df['CD_CONTA_CONTABIL'].str.startswith('4')
                                    
                                    
                                    if 'DESCRICAO' in df.columns:
                                        filtro_texto = df['DESCRICAO'].astype(str).str.contains('EVENTO|SINISTRO', case=False, na=False)
                                        df_filtered = df[filtro & filtro_texto].copy()
                                    else:
                                        df_filtered = df[filtro].copy()
                                    
                                    if not df_filtered.empty:
                                        
                                        if df_filtered['VL_SALDO_FINAL'].dtype == 'O':
                                            df_filtered['VL_SALDO_FINAL'] = df_filtered['VL_SALDO_FINAL'].str.replace('.', '').str.replace(',', '.').astype(float)
                                        
                    
                                        df_filtered['CNPJ'] = "N/D"
                                        if 'REG_ANS' in df_filtered.columns:
                                            df_filtered['RazaoSocial'] = "Reg. ANS " + df_filtered['REG_ANS'].astype(str)
                                        else:
                                            df_filtered['RazaoSocial'] = "Desconhecida"
                                            
                                        df_filtered['Trimestre'] = t_dados['trimestre']
                                        df_filtered['Ano'] = t_dados['ano']
                                        df_filtered['ValorDespesas'] = df_filtered['VL_SALDO_FINAL']
                                        
                                        cols = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']
                                        dfs.append(df_filtered[cols])
                                        
                            except Exception as e:
                                print(f"    Erro ao ler arquivo {nomearquivo}: {e}")
        except Exception as e:
            print(f"  Erro no download do ZIP: {e}")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


print("Iniciando Scraping")
url_base_dados = encontrar_demonstracoes(BASE_URL)

if url_base_dados:
    trimestres = ultimos_trimestres(url_base_dados)
    
    all_data = []
    for t in trimestres:
        df_t = processar_trimestre(t)
        if not df_t.empty:
            all_data.append(df_t)
    if all_data:
        df_completo = pd.concat(all_data, ignore_index=True)
        
        len_antes = len(df_completo)
        full_df = df_completo[df_completo['ValorDespesas'] != 0]
        print(f"Registros processados: {len(full_df)} (Removidos {len_antes - len(df_completo)} zeros)")
        
        df_completo.to_csv(OUTPUT_CSV, index=False, sep=';', encoding='utf-8-sig')           
        print(full_df.head(1))
    else:
        print("Nenhum dado encontrado")
else:
    print("Não foi possível encontrar a pasta inicial")