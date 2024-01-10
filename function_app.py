import azure.functions as func
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, select, text, DateTime
from datetime import datetime

app = func.FunctionApp()


@app.schedule(schedule="0 0 12 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def gerar_parcela_contemplada(myTimer: func.TimerRequest) -> None:
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
    consulta_sql = f"SELECT id, datavencimento, valor_parcela, administradora FROM tab_contemplados WHERE DAY(datavencimento) = {dia_atual} AND vendida <> 1 and administradora <> 'HS Consórcios';"

    pagamentos = Table('pagamentos', metadata,
        Column('id', Integer, primary_key=True),
        Column('created_at', DateTime, default=datetime.now),
        Column('idContemplada', Integer),
        Column('status', String(50), default='Pendente')
    )

    # Executar a consulta
    with engine.connect() as conexao:
        resultados = conexao.execute(text(consulta_sql))
        logging.info(resultados)

        for linha in resultados:
            id_contemplada = linha.id 
            valor_parcela = linha.valor_parcela
            insert_sql = f"INSERT INTO pagamentos (created_at, idContemplada, status, idSolicitante, valor_total, tipo_pagamento) VALUES (:created, :contemplada_id, :status, :solicitante, :valor, :tipo)"
            conexao.execute(text(insert_sql), {"created": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "contemplada_id": id_contemplada, "status": "Pendente", "solicitante": 45, "valor": valor_parcela , "tipo": "Pagamento parcela contemplada"})
            conexao.commit()
    # Fechar a conexão com o banco de dados
    conexao.close() 

@app.schedule(schedule="0 0 */4 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False)
def atualizar_contempladas_parceiro(myTimer: func.TimerRequest) -> None:
    engine = create_engine('mysql+pymysql://fragaebitelloc02:NovoProjeto2023@mysql.fragaebitelloconsorcios.com.br/fragaebitelloc02')
    metadata = MetaData()

    engine_2 = create_engine('mysql+pymysql://fragaebitelloc:Fb1234@mysql.fragaebitelloconsorcios.com.br/fragaebitelloc')
    metadata_2 = MetaData()

    # Montar a consulta SQL
    consulta_sql = """
        SELECT 
            id, 
            replace(replace(replace(FORMAT(valor_credito, 2), ".", "x"), ",", "."), "x",",") as valor_credito,
            replace(replace(replace(FORMAT(valor_credito_original, 2), ".", "x"), ",", "."), "x",",") as valor_credito_original, 
            categoria, 
            replace(replace(replace(FORMAT(entrada, 2), ".", "x"), ",", "."), "x",",") as entrada, 
            replace(replace(replace(FORMAT(parcelas, 2), ".", "x"), ",", "."), "x",",") as parcelas, 
            replace(replace(replace(FORMAT(valor_parcela, 2), ".", "x"), ",", "."), "x",",") as valor_parcela, 
            administradora, 
            cpf_cnpj as interno,
            cod, 
            reserva, 
            replace(replace(replace(FORMAT(fundo, 2), ".", "x"), ",", "."), "x",",") as fundo, 
            replace(replace(replace(FORMAT(parc, 2), ".", "x"), ",", "."), "x",",") as parc, 
            exclusivo, IF(visivel = "1", 'S', 'N') as visivel, 
            replace(replace(replace(FORMAT(precominimo, 2), ".", "x"), ",", "."), "x",",") as precominimo, 
            vendida
    FROM tab_contemplados
    WHERE visivel <> "N"
    AND exclusivo <> "S"
    AND vendida <> 1
    AND pre_compra <> 1"""

    # Executar a consulta
    with engine.connect() as conexao:
        resultados = conexao.execute(text(consulta_sql))
        conexao.commit()
    
    with engine_2.connect() as conexao_parceiro:
        truncate_sql = f"TRUNCATE TABLE tab_contemplados"
        conexao_parceiro.execute(text(truncate_sql))
        conexao_parceiro.commit()
        for linha in resultados:
            insert_sql = f"INSERT INTO tab_contemplados (id,valor_credito,valor_credito_original,categoria,entrada,parcelas,valor_parcela,administradora,interno,cod,reserva,fundo,parc,exclusivo,visivel,precominimo,vendida) VALUES (:id,:valor_credito,:valor_credito_original,:categoria,:entrada,:parcelas,:valor_parcela,:administradora,:interno,:cod,:reserva,:fundo,:parc,:exclusivo,:visivel,:precominimo,:vendida)"
            conexao_parceiro.execute(text(insert_sql), {"id": linha.id, "valor_credito": linha.valor_credito, "valor_credito_original": linha.valor_credito_original, "categoria": linha.categoria, "entrada": linha.entrada, "parcelas": linha.parcelas, "valor_parcela": linha.valor_parcela, "administradora": linha.administradora, "interno": linha.interno, "cod": linha.cod, "reserva": linha.reserva, "fundo": linha.fundo, "parc": linha.parc, "exclusivo": linha.exclusivo, "visivel": linha.visivel, "precominimo": linha.precominimo, "vendida": linha.vendida})
            conexao_parceiro.commit()
        update_sql_reservado = """
            UPDATE
                fragaebitelloc.tab_contemplados  
            SET 
                botao = '<button type="submit" class="btn btn-danger" formaction="../contemplados/mensagem.php">Reservado</button>',
                mobile = '<button type="submit" class="btn btn-danger mobiletable" formaction="../contemplados/mensagem.php"><i class="lnr fa fa fa-close"></i></button>'  
            WHERE 
                reserva = "Reservado";"""

        update_sql_disponivel = """
            UPDATE 
                fragaebitelloc.tab_contemplados
            SET 
                botao = '<button type="submit" class="btn btn-success" data-toggle="modal" data-target="#myModal">Reservar</button>',
                mobile = '<button type="submit" class="btn btn-success mobiletable" data-toggle="modal" data-target="#myModal"><i class="lnr fa fa-check-square-o"></i></button>'
            WHERE reserva = "Reservar";"""
            
        conexao_parceiro.execute(text(update_sql_reservado))
        conexao_parceiro.commit()

        conexao_parceiro.execute(text(update_sql_disponivel))
        conexao_parceiro.commit()

