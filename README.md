# teste-IntuitiveCare

- Parte 1
O PDF menciona que os arquivos podem ter diferentes formatos, mas apenas arquivos CSV existem para todos os semestres, além disso diz que O CSV que consolida os dados dos 3 trimestres deve conter as colunas: CNPJ, RazaoSocial, Trimestre, Ano, ValorDespesas mas nenhum arquivo dos anos e trimestres disponíveis tem os campos CNPJ nem Razão Social, o que consta no código é REG_ANS que é um Registro de Operadora de plano privado concedido pela ANS e CD_CONTA_CONTABIL que é um código que identifica a conta do plano de contas em vigor. O Teste também pede para acessar a API REST mas o site apresentado não apresenta API REST, então vou optar por uma abordagem de Web Scraping. Optei pelo processamento em DataFrames (Memória), mas processando trimestre por trimestre e consolidando no final. Isso é um meio-termo: não carrega tudo de uma vez (o que estouraria a RAM se fossem gigabytes), mas é mais rápido que linha a linha. 

Trade-off Técnico (Item 1.2):

Decisão: Processei cada trimestre independentemente e adicionei a uma lista all_data, consolidando apenas no final.

Justificativa: Como são apenas 3 trimestres de arquivos CSV/Excel (dados tabulares), o volume geralmente cabe na memória de um PC moderno (alguns GBs). Se fosse para baixar o histórico de 10 anos, eu teria optado por escrever o CSV linha a linha (stream) para o disco sem carregar na RAM. No código, usei io.BytesIO para baixar o ZIP na RAM sem salvar no disco antes de extrair, o que é mais rápido (I/O de disco é lento).

Análise de Inconsistências (Item 1.3):

CNPJ Duplicado: O código detecta se um mesmo CNPJ tem nomes diferentes (comum se a empresa mudou de nome no meio do ano). A "decisão crítica" implementada foi padronizar pelo primeiro nome encontrado.

Valores: O código converte formatação brasileira (1.000,00) para float Python, remove zeros (considerados irrelevantes para análise de despesa) mas mantém negativos (pois em contabilidade, estornos podem ser negativos e são dados válidos).

- Parte 2
Como não temos CNPJs no arquivo original e sim os registros ANS, devemos inverter a ordem das tarefas 2.1 e 2.2 o código primeiro baixa a base cadastral da ANS, usa o RegistroANS como chave para cruzar os dados e trazer o CNPJ, Razão Social real, UF e Modalidade e depois, com os CNPJs reais agora presentes, aplica a validação de formato e dígitos verificadores. A tarefa 2.3 é realizada normalmente.

Decisão de Chave de Cruzamento: Registro ANS
Embora o enunciado sugira o uso do CNPJ, optou-se pelo Registro ANS (REGISTRO_OPERADORA) como chave primária de ligação.

Motivo: O arquivo de origem das despesas não possuía uma coluna de CNPJ confiável estruturada, mas possuía o código da operadora embutido. Além disso, o Registro ANS é a chave imutável dentro do ecossistema da agência, enquanto CNPJs podem mudar em casos de reestruturação societária (matriz/filial), tornando o Registro ANS uma chave mais robusta para garantir o match.

Trade-off Técnico no JOIN (Tarefa 2.2)
Decisão: Foi utilizado um how='left' join, mantendo a tabela de despesas à esquerda.

Justificativa: O objetivo primário é analisar o volume financeiro de despesas. É comum que operadoras que tiveram despesas no passado (ex: em 2023) não constem mais no arquivo de "Operadoras Ativas" de hoje (foram liquidadas, fundidas, etc.). Se usássemos inner join, perderíamos esses registros financeiros históricos. Com left join, mantemos a despesa e marcamos os dados cadastrais como "N/D" (Não Disponível) para análise posterior.

Implementação da Validação de CNPJ (Tarefa 2.1)
A função validar_cnpj(cnpj) implementa o algoritmo oficial da Receita Federal. Ela calcula os dois dígitos verificadores baseados nos 12 primeiros números e pesos específicos, comparando com os dígitos fornecidos.

Trade-off Técnico na Validação (Tarefa 2.1)
Decisão: Registros com CNPJs inválidos (ou "N/D" por falta de match) NÃO foram excluídos. Foi criada uma coluna CNPJ_Valido (True/False).

Justificativa: Em auditoria de dados financeiros, um CNPJ inválido é um finding (um achado de auditoria), não um lixo para ser descartado. Excluir a linha significaria "esconder" milhões de reais em despesas que ocorreram sob um cadastro problemático. Mantemos o dado e o marcamos como suspeito (False) para que analistas possam filtrar se desejarem.

Agregação e Desafio Adicional (Tarefa 2.3)
O script utiliza o poder do groupby do Pandas.

Agregação Simples: Agrupa por RazaoSocial e UF e soma o ValorDespesas.

Desafio (Média/Desvio Padrão): Foi necessário um processo de duas etapas:

Primeiro, calcular a soma de despesas por trimestre para cada operadora (criando um sub-dataframe df_trimestral).

Depois, agrupar esse sub-dataframe por operadora para calcular a média (mean) e o desvio padrão (std) desses totais trimestrais. Isso mostra se os gastos da operadora são estáveis ou se têm picos muito grandes.

- Parte 3

A Decisão de Arquitetura (Trade-off: Normalização)

O desafio pede para escolhermos entre Tabela Única (Desnormalizada) ou Tabelas Separadas (Normalizadas).

Minha Escolha: Opção B - Tabelas Normalizadas Separadas.

Justificativa:

Organização: Imagine repetir o nome da operadora, o CNPJ e o endereço em cada linha de despesa. Se a operadora tiver 1.000 despesas, você repete o nome 1.000 vezes. Isso gasta espaço e gera erro (se digitar um nome errado, quebra o agrupamento).

Normalização: Nós separamos "Quem é a empresa" (Cadastro) de "O que ela gastou" (Despesas). Ligamos as duas pelo registro_ans.

Performance: Para somar valores, o banco lê uma tabela mais "magra" (só números), o que é muito mais rápido.

A Decisão de Tipos de Dados (Trade-off)

Dinheiro: Usaremos DECIMAL(15, 2) e não FLOAT.

Por que? FLOAT é aproximado (matemática científica). DECIMAL é exato (matemática financeira). Em contabilidade, 1 centavo perdido é inaceitável.

Datas: Usaremos DATE e não VARCHAR (Texto).

Por que? Se a data for texto "01/02/2023", o computador acha que vem antes de "02/01/2023" (ordem alfabética). Se for DATE, ele entende o tempo cronológico corretamente.

Importação: Optei por usar python para importação dos dados para o banco de dados, por simplicidade.

Tratamento de Inconsistências (Análise Crítica):

Strings em campos numéricos: O Python já limpou isso na Parte 1.

Valores NULL: Se uma operadora não tem UF no cadastro, inserimos como NULL ou 'ND'. O SQL aceita NULL se não dissermos NOT NULL na criação da tabela.

Datas: O trimestre "1T2023" é texto, então guardamos como texto (VARCHAR), mas o campo ano guardamos como número (INT) para ordenar.

Query 1: Top 5 operadoras com maior crescimento de despesas
O Desafio: Como comparar o primeiro e o último trimestre se algumas empresas não têm dados em todos? A Lógica:

Para cada empresa, descobrimos qual foi a primeira data que ela apareceu e a última data.

Pegamos o valor gasto nessas duas datas.

Fórmula: (Valor Final - Valor Inicial) / Valor Inicial.

Justificativa para dados faltantes: Se a empresa não tem dado no primeiro trimestre analisado, ela não pode entrar no cálculo de "crescimento", pois seria divisão por zero ou crescimento infinito (começou do nada). Filtramos apenas quem tem dados em ambos os pontas.

Query 2: Distribuição por UF + Desafio (Média por Operadora)
Explicação: Aqui usamos GROUP BY para "agrupar" as linhas por estado. O desafio pede a média por operadora dentro da UF, não a média geral da UF.

Aqui está uma versão mais concisa e direta, seguindo o mesmo estilo dos itens anteriores do seu README:

Query 3: Operadoras com Despesas Consistentemente Altas
Objetivo: Identificar operadoras que superaram a média global de despesas em pelo menos 2 dos 3 trimestres analisados.

Estratégia: Utilização de CROSS JOIN com uma subquery para calcular a média escalar do mercado e compará-la registro a registro.

Justificativa Técnica (Integridade de Dados):

Optou-se pelo uso de COUNT(DISTINCT d.trimestre) em vez de um COUNT simples. Essa abordagem garante a precisão da análise mesmo em cenários de inconsistência ou duplicação de dados no banco, assegurando que um mesmo trimestre não seja contabilizado múltiplas vezes para a mesma operadora.

A filtragem via WHERE antes do agrupamento otimiza a performance ao descartar despesas baixas precocemente.

- Parte 4

Escolha do Framework: Flask (Opção A)

"Optei pelo Flask (Opção A) devido à sua simplicidade e curva de aprendizado suave para o escopo deste teste. Enquanto o FastAPI oferece melhor performance assíncrona, o Flask é maduro, robusto e perfeitamente capaz de lidar com o volume de dados do teste sem a complexidade adicional de tipagem estática (Pydantic) exigida pelo FastAPI. A facilidade de integração com SQLAlchemy foi um fator decisivo."

Estratégia de Paginação: Offset-based (Opção A)

"Utilizei Paginação via Offset (LIMIT x OFFSET y). Justificativa: É a implementação mais simples e compatível com interfaces de tabela padrão (Página 1, 2, 3). Embora a paginação por Cursor (Keyset) seja mais performática para milhões de registros, para o dataset atual da ANS (alguns milhares de linhas), o Offset não apresenta degradação de performance perceptível e facilita a navegação aleatória pelo usuário."

Cache vs Queries Diretas: Queries Diretas (Opção A)

"Escolhi Calcular sempre na hora (Opção A). Justificativa: Os dados da ANS são atualizados trimestralmente. Em um cenário real de baixa frequência de atualização de dados, a implementação de uma camada de Cache (como Redis) adicionaria complexidade de infraestrutura desnecessária para este teste. O banco de dados MySQL, com os índices criados na etapa anterior, é suficiente para responder às agregações em milissegundos."

Estratégia de Busca: Servidor (Opção A)

"A busca é realizada no Servidor via SQL (LIKE %...%). Justificativa: Trazer todos os dados para o cliente (Front-end) filtrar sobrecarregaria o navegador e consumiria banda excessiva. Filtrar no banco de dados é mais eficiente e escalável."


- Instruções de execução:
Rodar o scraping.py para gerar o CSV resultado_final.csv, o código contempla toda a parte 1

Rodar o validacao.py para gerar os CSVs relatorio_agregado.csv e dados_enriquecidos_validados.csv, o código contempla toda a parte 2.

Executar criar_banco.sql para criar o banco de dados "teste_ans", rodar então o arquivo importar_banco.py, trocando o campo SENHA_MYSQL pela senha do banco do MySQL, rodar então cada query no arquivo queries.sql, isso contempla toda a parte 3.

Executar api.py(trocar SENHA_MYSQL pela senha do MySQL) e executar index.html, isso contempla toda a parte 4.