import azure.functions as func
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, select, text, DateTime
from datetime import datetime

app = func.FunctionApp()


@app.schedule(schedule="0 0 8 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def execute_time(myTimer: func.TimerRequest) -> None:
    # Conectar ao banco de dados MySQL
    engine = create_engine('mysql+pymysql://fragaebitelloc02:NovoProjeto2023@mysql.fragaebitelloconsorcios.com.br/fragaebitelloc02')
    conexao = engine.connect()
    metadata = MetaData()

    # Definir a tabela
    data_atual = datetime.now()
    dia_atual = data_atual.day
    mes_atual = data_atual.month
    ano_atual = data_atual.year

    # Montar a consulta SQL
    consulta_sql = f"SELECT id, datavencimento, valor_parcela, administradora FROM tab_contemplados WHERE DAY(datavencimento) = {dia_atual} AND MONTH(datavencimento) = {mes_atual} and YEAR(datavencimento) >= {ano_atual} and vendida <> 1 administradora <> 'HS Consórcios';"

    pagamentos = Table('pagamentos', metadata,
        Column('id', Integer, primary_key=True),
        Column('created_at', DateTime, default=datetime.now),
        Column('idContemplada', Integer),
        Column('status', String(50), default='Pendente')
    )

    # Executar a consulta
    with engine.connect() as conexao:
        resultados = conexao.execute(text(consulta_sql))

        for linha in resultados:
            id_contemplada = linha.id 
            valor_parcela = linha.valor_parcela
            insert_sql = f"INSERT INTO pagamentos (created_at, idContemplada, status, idSolicitante, valor_total, tipo_pagamento) VALUES (:created, :contemplada_id, :status, :solicitante, :valor, :tipo)"
            conexao.execute(text(insert_sql), {"created": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "contemplada_id": id_contemplada, "status": "Pendente", "solicitante": 45, "valor": valor_parcela , "tipo": "Pagamento parcela contemplada"})
            conexao.commit()
    # Fechar a conexão com o banco de dados
    conexao.close()



