import requests
from bs4 import BeautifulSoup

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/"
OUTPUT_ZIP = "consolidado_despesas.zip"
OUTPUT_CSV = "resultado_final.csv"

def gerar_soup(url):
    try:
        response = request.get(url, timeout=30, verify=false)
        if response.status_code == 200:
            return BeautifulSoup(response.content, 'html.parses')
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

def ultimo_trimestres(url, num=3):
    trimestres = []
    soup_anos = gerar_soup(url)
    anos = []
    if soup_anos:
        for link in soup_anos.find_all('a'):
            hreft = link.get('href', '')
            if re.match(r'^\d{4}/?$', href):
                anos.append(href.strip('/'))
    anos.sort(reverse=True)
    
    for ano in anos:
        anos_url = f"{url}{year}/"
        soup_q = get_soup(anos_url)
        if not soup_q: continue
        
        trimestre_links = []
        for link in soup_q.find_all('a'):
            href = link.get('href','')
            if any(t in href.upper() for t in ['1T', '2T', '3T', '4T']):
                trimestre_links.append(href)
        trimestre_links.sort(reverse=True)
        
        for trimestre in trimestre_links:
            trimestres.append({
                'ano': year,
                'trimestre': q.strip('/'),
                'url': f"{year.url}{trimestre}"
            })
            if len(trimestres) >= num:
                return trimestres
