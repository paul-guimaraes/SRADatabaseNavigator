import logging
import pickle
from configparser import RawConfigParser
from datetime import datetime
from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from os import makedirs, path, listdir
import pandas as pd
from tempfile import NamedTemporaryFile
from zipfile import ZipFile
import json
import time

from .data.database import Database
from .data.util import format_name
from .data.network import Network
from .data.util import dict_to_csv

work_directory = '/tmp/sra_network'


@require_http_methods(['POST'])
def add_table_join_fields(request):
    data = json.loads(request.body)
    for field in ('table', 'fields'):
        if field not in data:
            return HttpResponse(status=422)
    schema, table = data['table'].split('.')
    fields = data['fields']
    database = get_database()
    if database.insert_table_join_fields(schema, table, fields[0], schema, table, fields[1]):
        return HttpResponse(status=200)
    else:
        return HttpResponse(status=500)


def download_job(request, result):
    if 'result_directory' not in request.session:
        return HttpResponse(status=403)
    if result not in request.session['result_directory']:
        return HttpResponse(status=403)

    with NamedTemporaryFile() as temp_file:
        zip_result_file = ZipFile(temp_file, 'w')
        for file in listdir(path.join(work_directory, result)):
            zip_result_file.write(path.join(work_directory, result, file), file)
        zip_result_file.close()
        response = HttpResponse(
            content_type="application/x-zip-compressed",
            headers={"Content-Disposition": f'attachment; filename="jobs.zip"'},
            status=200,
        )
        with open(temp_file.name, 'rb') as file_response:
            response.write(file_response.read())
        return response


def get_database() -> Database:
    configparser = RawConfigParser()
    with open(path.join(path.dirname(__file__), '..', '..', 'config.ini'), 'r') as configfile:
        configparser.read_file(configfile)
    if not configparser.has_section('database'):
        raise Exception('Database section not found in config.ini.')
    database = Database(
        host=configparser['database']['host'],
        port=configparser['database']['port'],
        database=configparser['database']['name'],
        user=configparser['database']['user'],
        password=configparser['database']['password'],
    )
    return database


def index(request):
    return render(request, 'database/panel.html', {'menus': [
        'home',
        'filter',
        'join fields',
        'my job',
        # 'settings',
    ]})


@require_http_methods(['POST'])
def get_possible_equals_columns(request):
    data = json.loads(request.body)
    for field in ('table',):
        if field not in data:
            return HttpResponse(status=422)
    database = get_database()
    schema, table = data['table'].split('.')
    columns = database.search_by_equal_column(schema, table)
    return HttpResponse(json.dumps(columns), content_type='application/json')


@require_http_methods(['POST'])
def get_network(request):
    data = json.loads(request.body)
    for field in ('target_table',):
        if field not in data:
            return HttpResponse(status=422)

    target_table = data['target_table']
    result_directory = f"network_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"

    database = get_database()

    def __concat_data(_current_data, _all_data):
        size = 0 if len(_all_data) == 0 else len(_all_data[list(_all_data.keys())[0]])
        for _column in _current_data:
            if _column not in _all_data:
                _all_data[_column] = [None] * size
            _all_data[_column].extend(_current_data[_column])
        for _column in _all_data:
            novo_tamanho = len(_all_data[_column])
            if size < novo_tamanho:
                size = novo_tamanho
        for _column in _all_data:
            if len(_all_data[_column]) < size:
                _all_data[_column].extend([None] * (size - len(_all_data[_column])))
        return _all_data

    def __get_dict_table_data(_rows, _join_columns):
        _rows = [i for i in __join_columns(_rows, _join_columns)]
        temp_keys = set()
        for i in _rows:
            for k in i.keys():
                temp_keys.add(k)
        _current_data = {k: [] for k in temp_keys}
        for _row in _rows:
            for k in temp_keys:
                if k in _row:
                    _current_data[k].append(_row[k])
                else:
                    _current_data[k].append(None)
        return _current_data

    def __make_directory_results():
        if not path.exists(work_directory):
            makedirs(work_directory)
        makedirs(path.join(work_directory, result_directory))

    def __join_columns(_rows, _join_columns) -> dict:
        for _row in _rows:
            keys = [_column for _column in _row.keys()]
            for _column in keys:
                if _column in _join_columns['keys']:
                    new_column = _join_columns['keys'][_column]
                    if new_column not in _row or _row[new_column] is None:
                        _row[new_column] = _row[_column]
                    elif _row[new_column] != _row[_column]:
                        yield {'accession': _row['accession'], new_column: _row[_column]}
                    del _row[_column]
            yield _row

    def __remove_empty(_data, _key):
        # removendo colunas vazias
        for _column in set(_data.keys()):
            if len(set(_data[_column])) <= 1:
                del _data[_column]

        # removendo linhas vazias
        _ids_del = set()
        for i in range(len(_data[_key])):
            for _column in _data:
                if _column != _key and _data[_column][i] is not None:
                    break
            else:
                _ids_del.add(i)
        _temp = {}
        for _column in list(_data.keys()):
            _temp[_column] = [_value for _i, _value in enumerate(_data[_column]) if _i not in _ids_del]
            del _data[_column]
        _data = _temp
        del _temp
        return _data

    def __to_lower_data(_data):
        for _column in _data:
            for _i, _value in enumerate(_data[_column]):
                if isinstance(_value, str):
                    _data[_column][_i] = _value.lower()
        return _data

    def __update_session_status(status: str):
        print(status)
        if 'result_directory' not in request.session:
            request.session['result_directory'] = {}
        request.session['result_directory'][result_directory] = {
            'status': status,
        }
        request.session.save()

    __update_session_status(f'Getting network...')

    if len(data['tables']) or len(data['columns']) or len(data['mining_tables']) or len(data['mining_columns']):
        __update_session_status('Running')
        __make_directory_results()

    all_data = dict()
    start_execution = time.time()

    temp_ignored_fields = database.get_ignored_fields_with_schema()
    ignored_tables = {}
    ignored_columns = {}
    for table in temp_ignored_fields['tables']:
        schema = table['schema']
        table = table['table']
        if schema not in ignored_tables:
            ignored_tables[schema] = {table}
        else:
            ignored_tables[schema].add(table)
    for table in temp_ignored_fields['columns']:
        schema = table['schema']
        column = table['column']
        table = table['table']
        if schema not in ignored_columns:
            ignored_columns[schema] = {table: {column}}
        elif table not in ignored_columns[schema]:
            ignored_columns[schema][table] = {column}
        else:
            ignored_columns[schema][table].add(column)

    if len(data['tables']) > 0:
        for table in data['tables']:
            if table not in ignored_tables['public']:
                __update_session_status(f'Querying table {table}...')
                table_columns = database.get_tables_columns(table, data_mining_tables=False)
                table_columns = {column for column in table_columns['columns']}
                if 'public' in ignored_columns:
                    if table in ignored_columns['public']:
                        removed_columns = {column for column in ignored_columns['public'][table]}
                        table_columns = table_columns - removed_columns
                if table_columns:
                    rows = database.get_table_related_data(
                        table=table,
                        reference_table=target_table,
                        table_columns_filter=table_columns,
                    )

                    # combinando colunas marcadas na tabela table_join_fields
                    join_columns = database.get_table_join_fields('public', table)
                    current_data = __get_dict_table_data(rows, join_columns)

                    table_columns = [f'{target_table}_accession'] + [f'{table}_{column}' for column in sorted(table_columns)]
                    for column in list(current_data.keys()):
                        if column not in table_columns:
                            del current_data[column]

                    current_data = __remove_empty(current_data, f'{target_table}_accession')
                    current_data = __to_lower_data(current_data)

                    with open(path.join(work_directory, result_directory,
                                        f'input_graph_tables_{table}.csv'), 'w') as file:
                        dict_to_csv(current_data, file)

                    all_data = __concat_data(current_data, all_data)
                    del current_data

    if len(data['columns']) > 0:
        for table in data['columns']:
            if table not in ignored_tables['public']:
                columns = data['columns'][table]
                if table in ignored_columns['public']:
                    columns = [column for column in columns if column not in ignored_columns['public'][table]]

                __update_session_status(f'Querying {len(columns)} column(s) from {table}...')

                # adicionando colunas combinadas caso elas não entre na pesquisa
                join_columns = database.get_table_join_fields(schema='public', table=table)
                temp_add = set()
                for column in columns:
                    if column in join_columns['combinations']:
                        temp_add = temp_add | join_columns['combinations'][column]
                    elif column in join_columns['keys']:
                        temp_add = temp_add | join_columns['combinations'][join_columns['keys'][column]]

                for column in temp_add:
                    if column not in columns:
                        columns.append(column)

                if len(columns) > 0:
                    rows = database.get_table_related_data(
                        table=table,
                        reference_table=target_table,
                        table_columns_filter=columns
                    )
                    # combinando colunas marcadas na tabela table_join_fields
                    join_columns = database.get_table_join_fields('public', table)

                    current_data = __get_dict_table_data(rows, join_columns)

                    selected_columns = [f'{target_table}_accession'] + [f'{table}_{i}' for i in columns]

                    for column in list(current_data.keys()):
                        if column not in selected_columns:
                            del current_data[column]

                    current_data = __remove_empty(current_data, f'{target_table}_accession')
                    current_data = __to_lower_data(current_data)

                    with open(path.join(work_directory, result_directory,
                                        f'input_graph_tables_columns_{table}.csv'), 'w') as file:
                        dict_to_csv(current_data, file)

                    all_data = __concat_data(current_data, all_data)
                    del current_data

    if len(data['mining_tables']) > 0:
        for schema in data['mining_tables']:
            for table in data['mining_tables'][schema]:
                if schema in ignored_tables and table in ignored_tables[schema]:
                    continue
                __update_session_status(f'Querying mining table {table}...')
                # combinando colunas marcadas na tabela table_join_fields
                join_columns = database.get_table_join_fields(schema, table)
                rows = database.get_table_data(schema, table)

                current_data = __get_dict_table_data(rows, join_columns)

                if database.mining_references[table]['reference_table'] == target_table:
                    current_data[f'{target_table}_accession'] = current_data.pop('accession')
                else:
                    temp = database.get_mining_related_data(
                        consolidated_table=table,
                        referenced_table=database.mining_references[table]['reference_table'],
                        table_columns_filter={"accession"},
                    )

                    internal_ids = set()
                    for row in temp:
                        internal_ids.add(row[f"{database.mining_references[table]['reference_table']}_internal_id"])
                    accessions = []
                    for row in database.get_table_related_data(
                            table=database.mining_references[table]['reference_table'],
                            reference_table=target_table,
                            table_internal_id=list(internal_ids),
                    ):
                        accessions.append({
                            'accession': row[f"{database.mining_references[table]['reference_table']}_accession"],
                            f'{target_table}_accession': row[f'{target_table}_accession'],
                        })

                    current_data[f'{target_table}_accession'] = []
                    for accession in current_data['accession']:
                        for i, item in enumerate(accessions):
                            if item['accession'] == accession:
                                current_data[f'{target_table}_accession'].append(item[f'{target_table}_accession'])
                                del accessions[i]
                                break
                        else:
                            current_data[f'{target_table}_accession'].append(None)

                    del current_data['accession']

                if schema in ignored_columns:
                    if table in ignored_columns[schema]:
                        for column in list(current_data.keys()):
                            if column in ignored_columns[schema][table]:
                                del current_data[column]

                current_data = __remove_empty(current_data, f'{target_table}_accession')
                current_data = __to_lower_data(current_data)

                with open(path.join(work_directory, result_directory,
                                    f'input_graph_mining_tables_{schema}_{table}.csv'), 'w') as file:
                    dict_to_csv(current_data, file)

                all_data = __concat_data(current_data, all_data)
                del current_data

    if len(data['mining_columns']) > 0:
        for schema in data['mining_columns']:
            for table in data['mining_columns'][schema]:
                if schema in ignored_tables and table in ignored_tables[schema]:
                    continue
                columns = data['mining_columns'][schema][table]
                __update_session_status(f'Querying {len(columns)} columns from mining table {table}...')

                # adicionando colunas combinadas caso elas não entre na pesquisa
                join_columns = database.get_table_join_fields(schema=schema, table=table)
                temp_add = set()
                for column in columns:
                    if column in join_columns['combinations']:
                        temp_add = temp_add | join_columns['combinations'][column]
                    elif column in join_columns['keys']:
                        temp_add = temp_add | join_columns['combinations'][join_columns['keys'][column]]

                for column in temp_add:
                    if column not in columns:
                        columns.append(column)

                if schema in ignored_columns:
                    if table in ignored_columns[schema]:
                        columns = [column for column in columns if column not in ignored_columns[schema][table]]
                if 'accession' not in columns:
                    columns.append('accession')

                # combinando colunas marcadas na tabela table_join_fields
                join_columns = database.get_table_join_fields(schema, table)
                rows = database.get_table_data(schema, table, columns)
                current_data = __get_dict_table_data(rows, join_columns)

                if database.mining_references[table]['reference_table'] == target_table:
                    current_data[f'{target_table}_accession'] = current_data.pop('accession')
                else:
                    temp = database.get_mining_related_data(
                        consolidated_table=table,
                        referenced_table=database.mining_references[table]['reference_table'],
                        table_columns_filter={"accession"},
                    )

                    internal_ids = set()
                    for row in temp:
                        internal_ids.add(row[f"{database.mining_references[table]['reference_table']}_internal_id"])
                    accessions = []
                    for row in database.get_table_related_data(
                            table=database.mining_references[table]['reference_table'],
                            reference_table=target_table,
                            table_internal_id=list(internal_ids),
                    ):
                        accessions.append({
                            'accession': row[f"{database.mining_references[table]['reference_table']}_accession"],
                            f'{target_table}_accession': row[f'{target_table}_accession'],
                        })

                    current_data[f'{target_table}_accession'] = []
                    for accession in current_data['accession']:
                        for i, item in enumerate(accessions):
                            if item['accession'] == accession:
                                current_data[f'{target_table}_accession'].append(item[f'{target_table}_accession'])
                                del accessions[i]
                                break
                        else:
                            current_data[f'{target_table}_accession'].append(None)

                    del current_data['accession']

                current_data = __remove_empty(current_data, f'{target_table}_accession')
                current_data = __to_lower_data(current_data)

                with open(path.join(work_directory, result_directory,
                                    f'input_graph_mining_columns_{schema}_{table}.csv'), 'w') as file:
                    dict_to_csv(current_data, file)

                all_data = __concat_data(current_data, all_data)
                del current_data

    if len(all_data) == 0:
        return HttpResponse(status=204)

    with open(path.join(work_directory, result_directory, 'input_graph.csv'), 'w') as file:
        dict_to_csv(all_data, file)
    del all_data

    __update_session_status('Constructing network...')

    # TODO: colocar Network em Threads também no Django.
    network = Network(debug=settings.DEBUG, status_callback=__update_session_status, threading=False)
    network.generate_csv_index(
        csv_file=path.join(work_directory, result_directory, 'input_graph.csv'),
        column_key_name=f'{target_table}_accession')
    results = network.generate_network_from_csv(
        csv_file=path.join(work_directory, result_directory, 'input_graph.csv'),
        column_key_name=f'{target_table}_accession',
        show_labels=True)
    end_execution = time.time()

    # escrevendo resultados no disco
    # armazenando consulta
    with open(path.join(work_directory, result_directory, 'query.json'), 'w') as _file:
        _file.write(json.dumps({
            'mining_columns': data['mining_columns'],
            'time': str(end_execution - start_execution),
        }))
    html_index_template = ''
    with open(path.join(path.dirname(path.realpath(__file__)), 'data', 'template', 'network_index.html')) as file:
        html_index_template = file.read()

    result_summary = {
        'result_directory': result_directory,
        'network': {},
        'communities': []
    }

    # escrevendo subredes/comunidades
    communities_javascript_objects = []
    for i, result in enumerate(results['communities']):
        if isinstance(result, str):
            with open(result, 'rb') as tf:
                result = pickle.load(tf)
        nodes_number = str(result['info']['nodes_number'])
        edges_number = str(result['info']['edges_number'])
        type_connections = str(len(result['info']['labels']))

        result_summary['communities'].append({
            'nodes': result['info']['nodes_number'],
            'edges': result['info']['edges_number'],
            'type_connections': len(result['info']['labels'])
        })

        html_file_name = f'network_community_{i}.html'
        communities_javascript_objects.append(
            "{id: " + str(i)
            + ", nodes_number: " + nodes_number
            + ", edges_number: " + edges_number
            + ", type_connections: " + type_connections
            + ", description: 'labels " + ', '.join([str(label).replace("'", "\\'") for label in result['info']['labels']])
            + "', link: '" + html_file_name
            + "'}"
        )
        with open(path.join(work_directory, result_directory, html_file_name), 'w') as file:
            file.write(result['html'])
        with open(path.join(work_directory, result_directory, f'network_community_{i}.json'), 'w') as file:
            json.dump(result['table'], file)
        with open(path.join(work_directory, result_directory, f'network_community_{i}.csv'), 'w') as file:
            dict_to_csv(result['table'], file)

    if 'network' not in results:
        nodes_number = 0
        edges_number = 0
        type_connections_number = 0
        for i, result in enumerate(results['communities']):
            if isinstance(result, str):
                with open(result, 'rb') as tf:
                    result = pickle.load(tf)
            nodes_number += result['info']['nodes_number']
            edges_number += result['info']['edges_number']
            type_connections_number += len(result['info']['labels'])

        result_summary['network'] = {
            'nodes': nodes_number,
            'edges': edges_number,
            'type_connections': type_connections_number
        }

        html_index_template = html_index_template.replace(
            ':network:',
            "{nodes_number: " + str(nodes_number)
            + ", edges_number: " + str(edges_number)
            + ", description: 'Entire network.' "
            + ", type_connections: " + str(type_connections_number)
            + ", link: '#'}"
        )
    else:
        result = results['network']
        if isinstance(result, str):
            with open(result, 'rb') as tf:
                result = pickle.load(tf)

        result_summary['network'] = {
            'nodes': result['info']['nodes_number'],
            'edges': result['info']['edges_number'],
            'type_connections': len(result['info']['labels'])
        }

        nodes_number = str(result['info']['nodes_number'])
        edges_number = str(result['info']['edges_number'])
        type_connections_number = str(len(result['info']['labels']))
        html_file_name = f'network.html'
        html_index_template = html_index_template.replace(
            ':network:',
            "{nodes_number: " + nodes_number
            + ", edges_number: " + edges_number
            + ", type_connections: " + type_connections_number
            + ", description: 'Entire network.', "
            + "link: '" + html_file_name + "'}"
        )
        with open(path.join(work_directory, result_directory, html_file_name), 'w') as file:
            file.write(result['html'])
        with open(path.join(work_directory, result_directory,
                            f'network.json'), 'w') as file:
            json.dump(result['table'], file)
        with open(
                path.join(work_directory, result_directory, f'network.csv'),
                'w') as file:
            dict_to_csv(result['table'], file)

    html_index_template = html_index_template.replace(':communities:', ','.join(communities_javascript_objects))

    with open(path.join(work_directory, result_directory, f'index.html'), 'w') as file:
        file.write(html_index_template)

    __update_session_status('Finished')

    return render(request, 'database/get_network_result.html', result_summary)


@require_http_methods(['POST'])
def load_screen(request):
    data = json.loads(request.body)
    if 'screen' not in data:
        return HttpResponse(status=422)
    screen = data['screen']

    database = get_database()

    if screen == 'home':
        return render(request, 'database/home.html')
    elif screen == 'filter':
        temp = database.get_ignored_fields_with_schema()
        ignored_tables = {}
        for table in temp['tables']:
            schema = table['schema']
            table_name = table['table']
            if schema not in ignored_tables:
                ignored_tables[schema] = {table_name}
            else:
                ignored_tables[schema].add(table_name)
        ignored_fields = {}
        for table in temp['columns']:
            schema = table['schema']
            table_name = table['table']
            column = table['column']
            if schema not in ignored_fields:
                ignored_fields[schema] = {}
            if table_name not in ignored_fields[schema]:
                ignored_fields[schema][table_name] = {column}
            else:
                ignored_fields[schema][table_name].add(column)
        tables = database.get_tables()
        for _id, table, total in database.get_tables_count():
            tables[_id]['total'] = total
        columns = database.get_tables_columns(data_mining_tables=True)
        temp = {}
        other_columns = {}
        for table in columns:
            schema = columns[table]['schema']
            values = database.get_table_column_count(schema=schema, table_name=table, columns=columns[table]['columns'])
            for i, column in enumerate(columns[table]['columns']):
                column = {
                    'name': column,
                    'total': values[column],
                }
                columns[table]['columns'][i] = column
            if schema == 'public':
                temp[table] = columns[table]
            else:
                other_columns[table] = columns[table]
        columns = temp
        del temp
        # removing prohibited tables
        if 'public' in ignored_tables:
            for table in ignored_tables['public']:
                if table in columns:
                    columns[table]['columns'] = []
                if table in other_columns:
                    columns[table]['columns'] = []
        for schema in ignored_fields:
            for table in ignored_fields[schema]:
                join_fields = database.get_table_join_fields(schema=schema, table=table)
                # tratando campos bloqueados
                if schema == 'public':
                    if table in columns:
                        for exclude in ignored_fields[schema][table]:
                            if table in columns:
                                for column in columns[table]['columns']:
                                    if column['name'] == exclude:
                                        columns[table]['columns'].remove(column)
                                        break
                        # tratando campos combinados
                        for field, synonyms in join_fields['combinations'].items():
                            total = database.get_table_combined_column_count(schema=schema, table_name=table,
                                                                             columns=synonyms)
                            for column in columns[table]['columns']:
                                if column['name'] == field:
                                    column['total'] = total['total']
                                    break
                            for synonym in synonyms:
                                if field != synonym:
                                    for column in columns[table]['columns']:
                                        if column['name'] == synonym:
                                            columns[table]['columns'].remove(column)
                                            break
                else:
                    # tratando campos bloqueados
                    if table in other_columns:
                        for exclude in ignored_fields[schema][table]:
                            if table in other_columns:
                                for column in other_columns[table]['columns']:
                                    if column['name'] == exclude:
                                        other_columns[table]['columns'].remove(column)
                                        break
                        # tratando campos combinados
                        for field, synonyms in join_fields['combinations'].items():
                            total = database.get_table_combined_column_count(schema=schema, table_name=table, columns=synonyms)
                            for column in other_columns[table]['columns']:
                                if column['name'] == field:
                                    column['total'] = total['total']
                                    break
                            for synonym in synonyms:
                                if field != synonym:
                                    for column in other_columns[table]['columns']:
                                        if column['name'] == synonym:
                                            other_columns[table]['columns'].remove(column)
                                            break

        return render(request, 'database/filter.html', {
            'columns': columns,
            'mining_tables': other_columns,
            'tables': tables,
        })
    elif screen == 'join fields':
        all_tables = database.get_tables_columns(data_mining_tables=True)
        tables = []
        mining_tables = []
        for table in all_tables:
            schema = all_tables[table]['schema']
            name = all_tables[table]['name'].replace('_', ' ')
            if schema == 'public':
                tables.append({'schema': schema, 'table': table, 'name': name})
            else:
                mining_tables.append({'schema': schema, 'table': table, 'name': name})
        response = {'tables': tables, 'mining_tables': mining_tables}
        return render(request, 'database/join_fields.html', response)
    elif screen == 'my job':
        if 'result_directory' in request.session:
            response = {
                'jobs': request.session['result_directory'],
            }
            for job in response['jobs']:
                if path.exists(path.join(work_directory, job)) and response['jobs'][job]['status'].lower() == 'finished':
                    response['jobs'][job]['link'] = True
        else:
            response = {'jobs': None}
        return render(request, 'database/my_job.html', response)
    elif screen == 'settings':
        tables = database.get_tables()
        ignored_fields = database.get_ignored_fields_with_schema()
        for element in ignored_fields['tables']:
            if element['schema'] == 'public':
                element['table_name'] = tables[element['table']]['name']
        for element in ignored_fields['columns']:
            if element['schema'] == 'public':
                element['table_name'] = tables[element['table']]['name']
        return render(request, 'database/settings.html', {
            'database': database.get_database_name(),
            'ignored_fields': ignored_fields['columns'],
            'ignored_tables': ignored_fields['tables'],
            'tables': tables,
        })


@require_http_methods(['POST'])
def reject_table_join_fields(request):
    data = json.loads(request.body)
    for field in ('table', 'fields'):
        if field not in data:
            return HttpResponse(status=422)
    schema, table = data['table'].split('.')
    fields = data['fields']
    database = get_database()
    if database.insert_ignored_table_join_fields(schema, table, fields[0], schema, table, fields[1]):
        return HttpResponse(status=200)
    else:
        return HttpResponse(status=500)


@require_http_methods(['POST'])
def search_data(request):
    data = json.loads(request.body)
    for field in ('target_table',):
        if field not in data:
            return HttpResponse(status=422)

    # linkar buscas
    # 1) busca por presença de registro na tabela.
    # 2) busca por presença de registro em campo.
    # 3) busca por presença de registro em tabela consolidada.
    # 4) busca por presença de registro em campo consolidado.

    database = get_database()

    # Arquivos de resultados
    with NamedTemporaryFile() as temp_file:
        zip_result_file = ZipFile(temp_file, 'w')
        # Busca por presença de registro na tabela
        target_table = data['target_table']
        target_primary_key = f'{target_table}_internal_id'

        located_target_ids = set()
        intersect_located_target_ids = set()

        def add_results_to_zip(_rows, _zip_file: ZipFile, _zip_file_name: str):
            _results = []
            for _result in _rows:
                _results.append(_result)
            with NamedTemporaryFile() as _file:
                _dataframe = pd.DataFrame(_results)
                _dataframe.to_csv(_file, index=False)
                _zip_file.write(_file.name, _zip_file_name)

        def combine_results(_rows, _target_primary_key: str, _intersect_located_target_ids: set, _located_target_ids,
                            _zip_file: ZipFile, _zip_file_name: str):
            _ids = set()
            _result = []
            for _row in _rows:
                _located_target_ids.add(_row[_target_primary_key])
                _ids.add(_row[_target_primary_key])
                _result.append(_row)
            add_results_to_zip(_result, _zip_file, _zip_file_name)
            return _ids

        if len(data['tables']) > 0:
            for table in data['tables']:
                rows = database.get_table_related_data(table=table, reference_table=target_table)
                filename = format_name(f'{table}_x_{target_table}') + '.csv'
                temp_ids = combine_results(rows, target_primary_key, intersect_located_target_ids, located_target_ids,
                                           zip_result_file, filename)
                if len(intersect_located_target_ids):
                    intersect_located_target_ids = intersect_located_target_ids & temp_ids
                else:
                    intersect_located_target_ids = located_target_ids & temp_ids

        if len(data['columns']) > 0:
            for table in data['columns']:
                rows = database.get_table_related_data(table=table, table_columns_filter=data['columns'][table],
                                                       reference_table=target_table)
                filename = format_name(f'{table}-columns-{"-".join(data["columns"][table])}_x_{target_table}') + '.csv'
                temp_ids = combine_results(
                    rows, target_primary_key, intersect_located_target_ids, located_target_ids, zip_result_file,
                    filename)
                if len(intersect_located_target_ids):
                    intersect_located_target_ids = intersect_located_target_ids & temp_ids
                else:
                    intersect_located_target_ids = located_target_ids & temp_ids

        if len(data['mining_tables']) > 0:
            for schema in data['mining_tables']:
                for table in data['mining_tables'][schema]:
                    # TODO: ver como incluir essa tabela filtrada somente com as amostras que bateram.
                    # add_results_to_zip(
                    #     database.get_values_from_table(schema=schema, table=table),
                    #     zip_result_file, format_name(f'{schema}-{table}') + '.csv')
                    filename = format_name(f'{schema}-{table}_x_{target_table}') + '.csv'
                    temp_ids = combine_results(
                        database.get_mining_related_data(consolidated_table=table, referenced_table=target_table),
                        target_primary_key, intersect_located_target_ids, located_target_ids, zip_result_file, filename)
                    if len(intersect_located_target_ids):
                        intersect_located_target_ids = intersect_located_target_ids & temp_ids
                    else:
                        intersect_located_target_ids = located_target_ids & temp_ids

        if len(data['mining_columns']) > 0:
            for schema in data['mining_columns']:
                for table in data['mining_columns'][schema]:
                    columns = data['mining_columns'][schema][table]
                    rows = database.get_mining_related_data(
                        consolidated_table=table,
                        table_columns_filter=columns,
                        referenced_table=target_table,
                    )
                    filename = format_name(f'{schema}-{table}-columns-{"-".join(columns)}_x_{target_table}') + '.csv'
                    temp_ids = combine_results(
                        rows, target_primary_key, intersect_located_target_ids, located_target_ids, zip_result_file,
                        filename)
                    if len(intersect_located_target_ids):
                        intersect_located_target_ids = intersect_located_target_ids & temp_ids
                    else:
                        intersect_located_target_ids = located_target_ids & temp_ids

        if len(intersect_located_target_ids):
            filename = format_name(f'{target_table}') + '.csv'
            add_results_to_zip(
                database.get_values_from_table(table=target_table, schema='public', ids=intersect_located_target_ids),
                zip_result_file, filename)

        zip_result_file.close()
        response = HttpResponse(
            content_type="application/x-zip-compressed",
            headers={"Content-Disposition": f'attachment; filename="results.zip"'},
            status=200,
        )

        with open(temp_file.name, 'rb') as file_response:
            response.write(file_response.read())
        return response
