from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
import urllib.parse

app = Flask(__name__)
CORS(app) 

SENHA_MYSQL = "senha"  # Senha MySQL
senha_tratada = urllib.parse.quote_plus(SENHA_MYSQL)
if SENHA_MYSQL:
    db_str = f'mysql+pymysql://root:{senha_tratada}@localhost/teste_ans'
else:
    db_str = 'mysql+pymysql://root:@localhost/teste_ans'

engine = create_engine(db_str)

@app.route('/')
def home():
    return "O Servidor está ONLINE! 🚀"

@app.route('/api/operadoras', methods=['GET'])
def listar_operadoras():
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 10, type=int)
    search = request.args.get('search', '', type=str)
    
    offset = (page - 1) * limit

    sql = "SELECT * FROM operadoras WHERE 1=1"
    params = {'limit': limit, 'offset': offset}
    
    if search:
        sql += " AND razao_social LIKE :search"
        params['search'] = f"%{search}%"
    
    sql += " ORDER BY registro_ans LIMIT :limit OFFSET :offset"

    sql_count = "SELECT COUNT(*) FROM operadoras WHERE 1=1"
    if search:
        sql_count += " AND razao_social LIKE :search"

    with engine.connect() as conn:
        result = conn.execute(text(sql), params).mappings().all()
        total = conn.execute(text(sql_count), params).scalar()

    lista_operadoras = [dict(row) for row in result]

    return jsonify({
        "data": lista_operadoras,
        "total": total,
        "page": page,
        "limit": limit
    })

@app.route('/api/operadoras/<cnpj>', methods=['GET'])
def detalhe_operadora(cnpj):
    sql_op = "SELECT * FROM operadoras WHERE cnpj = :cnpj"
    
    with engine.connect() as conn:
        result = conn.execute(text(sql_op), {'cnpj': cnpj}).mappings().one_or_none()
        
        if not result:
            return jsonify({"erro": "Operadora não encontrada"}), 404
            
        return jsonify(dict(result))

@app.route('/api/operadoras/<cnpj>/despesas', methods=['GET'])
def despesas_operadora(cnpj):

    sql = """
    SELECT d.ano, d.trimestre, d.valor_despesa 
    FROM despesas d
    JOIN operadoras o ON d.registro_ans = o.registro_ans
    WHERE o.cnpj = :cnpj
    ORDER BY d.ano DESC, d.trimestre DESC
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(sql), {'cnpj': cnpj}).mappings().all()
        
    return jsonify([dict(row) for row in result])


@app.route('/api/estatisticas', methods=['GET'])
def estatisticas():
    sql_total = "SELECT SUM(valor_despesa) as total FROM despesas"
    sql_media = "SELECT AVG(valor_despesa) as media FROM despesas"
    
    sql_top5 = """
    SELECT o.razao_social, SUM(d.valor_despesa) as total
    FROM despesas d
    JOIN operadoras o ON d.registro_ans = o.registro_ans
    GROUP BY o.razao_social
    ORDER BY total DESC
    LIMIT 5
    """
    
    with engine.connect() as conn:
        total = conn.execute(text(sql_total)).scalar()
        media = conn.execute(text(sql_media)).scalar()
        top5 = conn.execute(text(sql_top5)).mappings().all()

    return jsonify({
        "total_despesas": float(total) if total else 0,
        "media_despesas": float(media) if media else 0,
        "top_5": [dict(row) for row in top5]
    })

if __name__ == '__main__':

    app.run(debug=True, port=5000)