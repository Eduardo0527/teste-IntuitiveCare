CREATE DATABASE IF NOT EXISTS teste_ans;
USE teste_ans;

CREATE TABLE operadoras (
    registro_ans INT PRIMARY KEY,        
    cnpj VARCHAR(20),                    
    razao_social VARCHAR(255),           
    modalidade VARCHAR(100),
    uf CHAR(2)                           
);

CREATE TABLE despesas (
    id INT AUTO_INCREMENT PRIMARY KEY, 
    registro_ans INT,                    
    trimestre VARCHAR(10),               
    ano INT,
    valor_despesa DECIMAL(18, 2),        
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    

    FOREIGN KEY (registro_ans) REFERENCES operadoras(registro_ans)
);

CREATE INDEX idx_despesas_ano ON despesas(ano);
CREATE INDEX idx_operadoras_uf ON operadoras(uf);