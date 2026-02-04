# Teste Técnico - IntuitiveCare

Este repositório contém a solução completa para o desafio técnico da IntuitiveCare. O projeto abrange o ciclo completo de engenharia de dados e desenvolvimento, desde a extração (Web Scraping/ETL), tratamento e validação, modelagem de banco de dados (SQL) até a exposição dos dados via API e Interface Web.

---

## 🛠 Tecnologias Utilizadas

- **Linguagem:** Python 3
- **Análise de Dados:** Pandas
- **Banco de Dados:** MySQL
- **Backend:** Flask (Python)
- **Frontend:** HTML5 + Vue.js (CDN)
- **Bibliotecas:** `requests`, `beautifulsoup4`, `sqlalchemy`, `pymysql`, `flask-cors`.

---

## 🚀 Parte 1: Extração e Transformação (ETL)

### 📥 Estratégia de Extração
O enunciado menciona que o CSV consolidado deveria conter colunas como `CNPJ` e `RazaoSocial`, porém os arquivos fonte (anos/trimestres) possuem apenas `REG_ANS` e `CD_CONTA_CONTABIL`. Além disso, devido à indisponibilidade da API REST mencionada no teste, optou-se por uma abordagem de **Web Scraping**.

**Processamento:**
O processamento foi realizado utilizando DataFrames em memória, iterando trimestre por trimestre e consolidando apenas ao final.

> **Trade-off Técnico (Item 1.2)**
>
> * **Decisão:** Processamento independente de cada trimestre, adicionando a uma lista `all_data` e consolidando no final.
> * **Justificativa:** Como o escopo abrange apenas 3 trimestres de dados tabulares, o volume cabe na memória de um PC moderno (alguns GBs). O carregamento total simultâneo estouraria a RAM se fossem muitos anos.
> * **Otimização:** Utilizei `io.BytesIO` para baixar o ZIP diretamente na RAM e extrair sem salvar em disco previamente, otimizando a performance dado que I/O de disco é geralmente o gargalo. Se fosse um histórico de 10 anos, a opção seria *stream* linha a linha para o disco.

### ⚠️ Tratamento de Inconsistências (Item 1.3)

* **CNPJ Duplicado:** O código detecta se um mesmo CNPJ possui nomes diferentes (comum em mudanças de Razão Social). A decisão crítica foi **padronizar pelo primeiro nome encontrado**.
* **Valores Monetários:** Conversão da formatação brasileira (`1.000,00`) para `float` Python. Zeros foram removidos (irrelevantes), mas **valores negativos foram mantidos**, pois representam estornos contábeis válidos.

---

## 🔄 Parte 2: Enriquecimento e Validação

Devido à ausência de CNPJs nos arquivos financeiros originais, a ordem das tarefas foi invertida: primeiro baixa-se a base cadastral para enriquecimento, cruza-se os dados e, por fim, valida-se.

### Estratégia de Cruzamento (Join)

> **Decisão de Chave:** **Registro ANS (`REGISTRO_OPERADORA`)**
> * **Motivo:** O arquivo de despesas possuía o código da operadora confiável. O Registro ANS é a chave imutável no ecossistema da agência, enquanto CNPJs podem mudar (reestruturação societária, matriz/filial), tornando o Registro ANS mais robusto.

> **Trade-off no JOIN (Tarefa 2.2)**
> * **Decisão:** `how='left'` join (Tabela de Despesas à esquerda).
> * **Justificativa:** O foco é o volume financeiro. Operadoras que tiveram despesas no passado mas foram liquidadas (não constam no arquivo de "Ativas" atual) devem ser contabilizadas. Um `inner join` perderia esse histórico. Dados cadastrais faltantes foram marcados como "N/D".

### ✅ Validação de CNPJ (Tarefa 2.1)

Implementação do algoritmo oficial da Receita Federal (cálculo de dígitos verificadores baseados em pesos e resto da divisão).

> **Trade-off na Validação**
> * **Decisão:** Registros com CNPJs inválidos ou não encontrados **NÃO** foram excluídos. Criou-se uma flag `CNPJ_Valido` (True/False).
> * **Justificativa:** Em auditoria, um dado inválido é um *finding* (achado), não lixo. Excluir a linha esconderia milhões em despesas ocorridas sob cadastros problemáticos. O dado é mantido e marcado para filtragem analítica posterior.

### 📊 Agregação (Tarefa 2.3)

Utilização do `groupby` do Pandas por `RazaoSocial` e `UF`.
* **Desafio Adicional:** Cálculo de Média e Desvio Padrão realizado em duas etapas (Soma por trimestre -> Agrupamento por operadora para `mean` e `std`) para identificar a estabilidade ou volatilidade dos gastos.

---

## 🗄️ Parte 3: Banco de Dados e SQL

### Arquitetura e Modelagem

> **Trade-off: Normalização**
> * **Escolha:** **Opção B - Tabelas Normalizadas Separadas.**
> * **Justificativa:**
>     1.  **Organização:** Evita repetir Razão Social, CNPJ e Endereço em cada linha de despesa (economia de espaço e integridade).
>     2.  **Performance:** As somas são feitas em uma tabela de fatos "magra" (apenas IDs numéricos e valores), o que é mais performático.

### Tipos de Dados
* **Dinheiro:** `DECIMAL(15, 2)`. *Motivo:* `FLOAT` é aproximado; `DECIMAL` é exato (financeiro). Perder centavos em contabilidade é inaceitável.
* **Datas:** `DATE`. *Motivo:* Garante ordenação cronológica correta, ao contrário de `VARCHAR`.

### Queries Analíticas Desenvolvidas

1.  **Top 5 Crescimento de Despesas:** `(Valor Final - Valor Inicial) / Valor Inicial`. Filtra apenas empresas com dados presentes em ambas as pontas.
2.  **Distribuição por UF:** Uso de `GROUP BY` por estado com cálculo de média por operadora dentro do agrupamento.
3.  **Operadoras com Despesas Consistentemente Altas (Query 3):**
    * *Estratégia:* `CROSS JOIN` com subquery de média escalar.
    * *Justificativa Técnica:* Optou-se por **`COUNT(DISTINCT d.trimestre)`** ao invés de `COUNT` simples. Isso garante a precisão mesmo se houver duplicação de dados na ingestão, assegurando que um mesmo trimestre não seja contabilizado múltiplas vezes.

---

## 🌐 Parte 4: API e Interface Web

### Backend (Flask)

> **Escolha do Framework: Flask (Opção A)**
>
> "Optei pelo Flask devido à sua simplicidade e maturidade. Enquanto o FastAPI oferece melhor performance assíncrona, o Flask é robusto e perfeitamente capaz de lidar com o volume de dados do teste sem a complexidade adicional de tipagem estática. A facilidade de integração com SQLAlchemy foi decisiva."

### Decisões de Arquitetura da API
1.  **Paginação:** **Offset-based (Opção A)**. Implementação simples (`LIMIT x OFFSET y`). Para o volume atual, não há degradação de performance que justifique Cursor-based.
2.  **Cache vs Queries Diretas:** **Queries Diretas (Opção A)**. Os dados da ANS são atualizados trimestralmente. Redis seria *over-engineering* para dados estáticos; MySQL responde em milissegundos.
3.  **Busca:** **Servidor (Opção A)** via SQL (`LIKE %...%`). Filtrar no client-side consumiria banda excessiva e sobrecarregaria o navegador.

---

## ▶️ Como Executar o Projeto

Siga a ordem abaixo para reproduzir a solução completa:

### 1. Extração (Parte 1)
Execute o script de scraping para baixar e consolidar os dados brutos.
```bash
python scraping.py
```

### 2. Validação e Enriquecimento (Parte 2)

Execute o script de validação para baixar os dados cadastrais, cruzar com as despesas e gerar os relatórios.

```bash
python validacao.py
```

## 3. Banco de Dados (Parte 3)

### A. Criação da Estrutura
Execute o script `criar_banco.sql` no seu cliente MySQL (Workbench, DBeaver, ou via terminal).
* Isso criará o banco de dados `teste_ans` e as tabelas necessárias.

### B. Importação dos Dados
1. Edite o arquivo `importar_banco.py`.
2. Troque o valor do campo `SENHA_MYSQL` pela sua senha do banco de dados.
3. Execute o script:

```bash
python importar_banco.py
```
### C. Análise SQL
Execute as queries contidas no arquivo `queries.sql` no seu cliente MySQL para visualizar os resultados das perguntas de negócio.

---

## 4. Servidor e Frontend (Parte 4)

### A. Configuração da API
1. Edite o arquivo `api.py`.
2. Troque o valor do campo `SENHA_MYSQL` pela sua senha do banco de dados.

### B. Iniciar Servidor
Execute o script abaixo para subir a API:

```bash
python api.py
```
> Aguarde a mensagem no terminal confirmando que o servidor está rodando na porta `5000`.

### C. Acessar Interface
Vá até a pasta do projeto e dê um duplo clique no arquivo `index.html` para abrir o Dashboard no seu navegador.