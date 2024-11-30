#!/usr/bin/env python3

from collections import Counter
from data.database import Database
from os import makedirs
import logging
from os import path
from configparser import RawConfigParser

if __name__ == "__main__":

    output_diretory = '/tmp/temp_result'

    if not path.exists(output_diretory):
        makedirs(output_diretory)

    logging.basicConfig(
        filename=f'{output_diretory}/checkdatabase.log',
        encoding='utf-8',
        format='%(levelname)s %(asctime)s: %(message)s',
        level=logging.INFO
    )

    with open(path.join(path.dirname(path.realpath(__file__)), '..', 'database', 'config.ini'), 'r') as config_file:
        configparser = RawConfigParser()
        configparser.read_file(config_file)
        database = Database(
            host=configparser['database']['host'],
            port=configparser['database']['port'],
            database=configparser['database']['name'],
            user=configparser['database']['user'],
            password=configparser['database']['password'],
        )

    database.create_annotation_tables()

    logging.info('Searching for tables with pair fields...')
    pair_fields = [('tag', 'value')]
    for pair_field in pair_fields:
        tables = database.get_tables_columns()
        for table in tables:
            if pair_field[0] in tables[table]['columns'] and pair_field[1] in tables[table]['columns']:
                database.insert_table_pair_fields(table, pair_field[0], pair_field[1])

    database.detect_data(cache=True)

    logging.info('Cheching for records numbers...')
    tabelas = {
        'table': [],
        'name': [],
        'records': [],
    }
    for table, name, records in database.get_tables_count():
        tabelas['table'].append(table)
        tabelas['name'].append(name)
        tabelas['records'].append(records)

    tables = database.get_tables_columns(show_type=True)
    columns = []

    for table in sorted(tables):
        for column in tables[table]['columns']:
            columns.append(column['name'])

    items = Counter(columns)
    for column in set(columns):
        if (column.startswith('experiment_package') or column.startswith('exp_pac')) and column.endswith('_id'):
            del items[column]
        elif column.endswith('_id'):
            del items[column]

    for column in (
            'label',
    ):
        del items[column]

    logging.info('Procurando sinônimos em campos tag/value...')
    results = database.get_counts_data(fields=[{'tag': 'value'}], cache=True)
    database.get_counts_file(results=results, output_dir=output_diretory)

    logging.info('Completando dados consolidados...')
    database.add_all_to_consolidated_mesh_terms_table()
    logging.info('Preparando tabela com termos MeSH...')
    database.create_sample_mining_terms_table()
    logging.info('Criando consolidado + termos MeSH para todas as amostras...')
    database.create_consolidated_mesh_terms_table()
    logging.info('Criando consolidado de tabelas especiais...')
    database.create_special_consolidate_sample()
    logging.info('Preparando tabelas de exceções...')
    database.create_ignored_fields_tables()

    logging.info('Processo finalizado.')
