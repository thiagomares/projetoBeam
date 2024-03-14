import apache_beam as beam
from apache_beam.dataframe.io import read_excel
from apache_beam.options.pipeline_options import PipelineOptions
import typing
from datetime import date, time
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import mysql.connector as my
from datetime import date


pipe_opts = PipelineOptions(argc=None)
pipeline = beam.Pipeline(options=pipe_opts)


def read_excel(excel_file_path, sheet_name):
    # Leitura do arquivo Excel usando pandas
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
    # Converta DataFrame para uma lista de linhas
    data = df.values.tolist()
    return data


def insert_data(data, tabela, campos):
    config = {
        'host': 'localhost',
        'database': 'dados_tratados',
        'user': 'thiagomares',
        'password': 'Ferreira13',
        'raise_on_warnings': True
    }
    dados = data

    try:
        conn = my.connect(**config)
        if conn.is_connected():
            print('Connected to MySQL database')
            cursor = conn.cursor()
            # Insert data into MySQL
            for row in data:
                cursor.execute(
                    f"INSERT INTO {tabela} (CIDADE, REGIONAL) VALUES {(row[0], row[1])}")
            conn.commit()
    except my.Error as e:
        print(e)
    finally:
        if 'conn' in locals() or 'conn' in globals():
            conn.close()
            print('MySQL connection is closed')
    return dados


def retorna_dados(ignorer):
    config = {
        'host': 'localhost',
        'database': 'dados_tratados',
        'user': 'thiagomares',
        'password': 'Ferreira13',
        'raise_on_warnings': True
    }

    try:
        conn = my.connect(**config)
        if conn.is_connected():
            print('Connected to MySQL database')
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM REGIONAIS')
            records = cursor.fetchall()
            print('Total de registros: ', cursor.rowcount)
            for row in records:
                print(row)
    except my.Error as e:
        print(e)
    finally:
        if 'conn' in locals() or 'conn' in globals():
            conn.close()
            print('MySQL connection is closed')
    return records


def agrupa_dicionario(dados):
    colunas = ['cpf', 'data_encaminhamento', 'hora_encaminhamento',
               'cidade', 'data_distribuicao', 'hora_distribuicao', 'renda', 'fgts']
    dados = pd.DataFrame(dados, columns=colunas)
    return dados.to_dict(orient='records')


def chave_cidade(dados):
    for chave in dados:
        yield (chave['cidade'], chave)


def converte_datetime(dados):
    dados['data_encaminhamento'] = pd.to_datetime(dados['data_encaminhamento'])
    dados['data_distribuicao'] = pd.to_datetime(dados['data_distribuicao'])
    return dados


# read to pcollection excel file
with beam.Pipeline(options=pipe_opts) as pipeline:

    df = (
        pipeline
        | 'Read Excel' >> beam.Create(['./dados/Prova-Excel - Vagas BH (1).xlsx'])
        | beam.Map(read_excel, sheet_name='Auxiliar - Cidade x Regional')
        # | 'enviando dados para o banco' >> beam.Map(insert_data, tabela='REGIONAIS', campos=('CIDADE', 'REGIONAL'))
        | beam.Map(retorna_dados)
        | beam.Map(print)
    )
    trata_valores = (
        pipeline
        | 'Read dados' >> beam.Create(['./dados/Prova-Excel - Vagas BH (1).xlsx'])
        | "carrega dados" >> beam.Map(read_excel, sheet_name='Base Encaminhamentos')
        | "convertendo para dicionario" >> beam.Map(agrupa_dicionario)
        | "cria chave cidade" >> beam.FlatMap(chave_cidade)
        | 'imprime valores' >> beam.Map(print)
    )
pipeline.run()
