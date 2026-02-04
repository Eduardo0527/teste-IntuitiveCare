

-- Query 1
WITH limites_trimestres AS (
    SELECT MIN(CONCAT(ano, trimestre)) as inicio, MAX(CONCAT(ano, trimestre)) as fim
    FROM despesas
),
valores_pontas AS (
    SELECT 
        d.registro_ans,
        o.razao_social,
        SUM(CASE WHEN CONCAT(d.ano, d.trimestre) = (SELECT inicio FROM limites_trimestres) THEN d.valor_despesa ELSE 0 END) as valor_inicial,
        SUM(CASE WHEN CONCAT(d.ano, d.trimestre) = (SELECT fim FROM limites_trimestres) THEN d.valor_despesa ELSE 0 END) as valor_final
    FROM despesas d
    JOIN operadoras o ON d.registro_ans = o.registro_ans
    GROUP BY d.registro_ans, o.razao_social
)
SELECT 
    razao_social,
    valor_inicial,
    valor_final,
    ((valor_final - valor_inicial) / valor_inicial) * 100 as crescimento_percentual
FROM valores_pontas
WHERE valor_inicial > 0 
ORDER BY crescimento_percentual DESC
LIMIT 5;

-- Query 2
SELECT 
    o.uf,
    SUM(d.valor_despesa) as despesa_total_estado,
    SUM(d.valor_despesa) / COUNT(DISTINCT d.registro_ans) as media_por_operadora
FROM despesas d
JOIN operadoras o ON d.registro_ans = o.registro_ans
WHERE o.uf IS NOT NULL
GROUP BY o.uf
ORDER BY despesa_total_estado DESC
LIMIT 5;

-- Query 3
SELECT 
    o.registro_ans,
    o.razao_social,
    o.uf,
    COUNT(DISTINCT d.trimestre) AS trimestres_acima_media
FROM despesas d
JOIN operadoras o 
    ON o.registro_ans = d.registro_ans
CROSS JOIN (
    SELECT AVG(valor_despesa) AS media_geral
    FROM despesas
) m
WHERE d.valor_despesa > m.media_geral
GROUP BY 
    o.registro_ans,
    o.razao_social,
    o.uf
HAVING COUNT(DISTINCT d.trimestre) >= 2
ORDER BY trimestres_acima_media DESC;
