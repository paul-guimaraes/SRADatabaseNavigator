from copy import deepcopy
from shutil import copyfile

from . import xml as XML
from .mining import Mining
from .util import format_name, NONE_VALUES, StatusBar
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
from enum import Enum
from enum import auto
from kmodes.kmodes import KModes
from kneed import KneeLocator
from lxml import etree
from multiprocessing import cpu_count
from nltk import Tree
from os import path
from sqlalchemy import create_engine
from tempfile import NamedTemporaryFile
from threading import Lock
import logging
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2


# Suported DBMS
class DBMS(Enum):
    POSTGRESQL = auto()


# Datbase interactions
class Database:
    static_mining = None

    def __init__(self, database, host='localhost', user=None, password=None, port=None, dbms=DBMS.POSTGRESQL,
                 schema='data_mining', limit_record_number=None, chunk_size=100000):
        self.__database = database
        self.__dbms = dbms
        self.__limit_record_number = limit_record_number
        self.__schema = schema
        self.__parameters = {
            'host': host,
            'database': database,
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 5,
            'keepalives_count': 5,
        }
        if user is not None:
            self.__parameters['user'] = user
        if password is not None:
            self.__parameters['password'] = password
        if port is not None:
            self.__parameters['port'] = port
        self.__chunk_size = chunk_size
        self.__connection = None
        self.__connection_2 = None
        if dbms == DBMS.POSTGRESQL:
            self.__connection = psycopg2.connect(**self.__parameters)
            self.__connection_2 = psycopg2.connect(**self.__parameters)
        self.mining = self.static_mining
        # TODO: check word size length to consider data as unstructured
        self.__word_size_length_unstructured = 10

        # TODO: incluir essas referências no banco de dados
        self.mining_references = {
            'consolidated_drug_sample': {'column': 'accession', 'reference_table': 'experiment_package_sample',
                                        'reference_column': 'accession'},
            'consolidated_experiment': {'column': 'accession', 'reference_table': 'experiment_package_experiment',
                                        'reference_column': 'accession'},
            'consolidated_run': {'column': 'accession', 'reference_table': 'experiment_package_run_set_run',
                                 'reference_column': 'accession'},
            'consolidated_sample': {'column': 'accession', 'reference_table': 'experiment_package_sample',
                                    'reference_column': 'accession'},
            'consolidated_sample_mining_terms': {'column': 'accession', 'reference_table': 'experiment_package_sample',
                                                 'reference_column': 'accession'},
            'consolidated_study': {'column': 'accession', 'reference_table': 'experiment_package_study',
                                   'reference_column': 'accession'},
            'consolidated_submission': {'column': 'accession', 'reference_table': 'experiment_package_submission',
                                        'reference_column': 'accession'},
        }

    def add_all_to_consolidated_mesh_terms_table(self):
        ignored_tables = {}
        ignored_columns = {}
        with self.__connection.cursor() as cursor, self.__connection.cursor() as cursor_2:
            ignored = self.get_ignored_fields_with_schema()
            # listando tabelas proibidas
            for item in ignored['tables']:
                schema = item['schema']
                table = item['table']
                if schema not in ignored_tables:
                    ignored_tables[schema] = {table}
                elif table not in ignored_tables[schema]:
                    ignored_tables[schema].add(table)
            # listando colunas proibidas
            for item in ignored['columns']:
                schema = item['schema']
                table = item['table']
                column = item['column']
                if schema not in ignored_columns:
                    ignored_columns[schema] = {table: {column}}
                elif table not in ignored_columns[schema]:
                    ignored_columns[schema][table] = {column}
                else:
                    ignored_columns[schema][table].add(column)
            # listando dados estruturados
            cursor.execute(f"""select schema_name, table_name, field from {self.__schema}.table_unstructured_field""")
            for row in self.__unpack_results(cursor):
                schema, table, field = row
                if schema not in ignored_columns:
                    ignored_columns[schema] = {}
                if table not in ignored_columns[schema]:
                    ignored_columns[schema][table] = {field}
                else:
                    ignored_columns[schema][table].add(field)
            # listando campos pares
            cursor.execute(f"""select 'public', table_name, field_a, field_b from {self.__schema}.table_pair_field""")
            for row in self.__unpack_results(cursor):
                schema, table, field_a, field_b = row
                if schema not in ignored_columns:
                    ignored_columns[schema] = {}
                if table not in ignored_columns[schema]:
                    ignored_columns[schema][table] = {field_a, field_b}
                else:
                    ignored_columns[schema][table].add(field_a)
                    ignored_columns[schema][table].add(field_b)

            reference_table = 'experiment_package_sample'
            temporary_table = f'temp_table_data'

            cursor.execute(f"""create temporary table {temporary_table}_ids (
                schema_name text,
                table_name text,
                column_name text,
                internal_id bigint,
                sample_accession text
            )""")

            cursor.execute(f"""
            create temporary table {temporary_table} (
                schema_name text,
                table_name text,
                column_name text,
                internal_id bigint,
                sample_accession text,
                value text
            )
            """)

            tables = self.get_tables_columns(show_type=False, data_mining_tables=False)
            with StatusBar('Loading tables data...', len(tables)) as status_bar:
                query_insert = f"""
                insert into {temporary_table}(schema_name, table_name, column_name, internal_id, sample_accession, value)
                values(%s, %s, %s, %s, %s, %s)
                """
                for table in tables:
                    schema = tables[table]['schema']
                    if schema in ignored_tables:
                        if table in ignored_tables[schema]:
                            continue
                    query = f"""select * from {schema}.{table}"""
                    if self.__limit_record_number is not None:
                        query = f"{query} order by random() limit {self.__limit_record_number}"
                    cursor_2.execute(query)
                    columns = [column.name for column in cursor_2.description]
                    for row in self.__unpack_results(cursor_2):
                        record = dict(zip(columns, row))
                        for column in columns:
                            if column != 'internal_id':
                                # ignorando campos proibidos ou não estruturados.
                                if schema in ignored_columns:
                                    if table in ignored_columns[schema]:
                                        if column in ignored_columns[schema][table]:
                                            continue
                                if record[column] is not None:
                                    cursor.execute(
                                        query_insert,
                                        [
                                            schema,
                                            table,
                                            column,
                                            record['internal_id'],
                                            None,  # sample_accesion
                                            record[column]
                                        ]
                                    )
                    status_bar.update()
            cursor_2.execute(f"""select 
                            schema_name, table_name, column_name, array_agg(distinct(internal_id)) as internal_ids 
                        from {temporary_table}
                        group by schema_name, table_name, column_name;""")
            with StatusBar('Recovering accession numbers...', cursor_2.rowcount + 1) as status_bar:
                query = f"""insert into {temporary_table}_ids (schema_name, table_name, column_name, internal_id, 
                sample_accession) values (%s, %s, %s, %s, %s);"""
                for schema_name, table_name, column_name, internal_ids in self.__unpack_results(cursor_2):
                    for sample in self.get_table_related_data(
                            table=table_name, table_internal_id=internal_ids,
                            reference_table=reference_table):
                        cursor.execute(query, (
                            schema_name,
                            table_name,
                            column_name,
                            sample[f'{table_name}_internal_id'],
                            sample[f'{reference_table}_accession']
                        ))
                    status_bar.update()
                cursor.execute(f"""
                update {temporary_table}
                set sample_accession = accessions.sample_accession
                from (
                    select
                        schema_name, table_name, column_name, internal_id, sample_accession
                    from {temporary_table}_ids
                    group by schema_name, table_name, column_name, internal_id, sample_accession
                ) as accessions
                where {temporary_table}.schema_name = accessions.schema_name
                    and {temporary_table}.table_name = accessions.table_name
                    and {temporary_table}.column_name = accessions.column_name
                    and {temporary_table}.internal_id = accessions.internal_id
                """)
                status_bar.update()

            consolidated = {}
            cursor.execute(f"""
            select sample_accession, column_name, value
            from {temporary_table}
            group by column_name, sample_accession, value
            """)
            query = f"""
            select * from {self.__schema}.consolidated_sample
            """
            if self.__limit_record_number is not None:
                query = f"""{query} order by random() limit {self.__limit_record_number}"""
            cursor_2.execute(query)
            with StatusBar('Preparing sample consolidated data...', cursor.rowcount + cursor_2.rowcount) as status_bar:
                for accession, column, value in self.__unpack_results(cursor):
                    if value is not None:
                        if accession not in consolidated:
                            consolidated[accession] = {column: {value}}
                        elif column not in consolidated[accession]:
                            consolidated[accession][column] = {value}
                        else:
                            consolidated[accession][column].add(value)
                    status_bar.update()
                columns = [column.name for column in cursor_2.description]
                for row in self.__unpack_results(cursor_2):  # checar sobre escritas
                    record = dict(zip(columns, row))
                    accession = record['accession']
                    for column in set(columns) - {'accession'}:
                        value = record[column]
                        if value is not None:
                            if accession not in consolidated:
                                consolidated[accession] = {column: {value}}
                            elif column not in consolidated[accession]:
                                consolidated[accession][column] = {value}
                            else:
                                consolidated[accession][column].add(value)
                    status_bar.update()

            with StatusBar('Writing data to database...', 5 + (len(consolidated) * 2)) as status_bar:
                columns = {}
                for accession, value in consolidated.items():
                    value['accession'] = accession
                status_bar.update()
                consolidated = [value for accession, value in consolidated.items()]
                status_bar.update()
                for row in consolidated:
                    temp = deepcopy(row)
                    for column, value in temp.items():
                        if isinstance(value, set):
                            type_column = columns[column] if column in columns else None
                            for i, item in enumerate(value):
                                if type_column != 'text':
                                    if not isinstance(item, str):
                                        print(f'{item} não é uma string.')
                                    else:
                                        if '-' in item and not item.startswith('-'):
                                            type_column = 'text'
                                        elif type_column != 'double precision' and item.replace('.', '', 1).replace('-',
                                                                                                                    '',
                                                                                                                    1).isnumeric():
                                            if '.' in item:
                                                type_column = 'double precision'
                                            else:
                                                type_column = 'bigint'
                                        else:
                                            type_column = 'text'
                                if i == 0:
                                    row[column] = deepcopy(item)
                                else:
                                    row_temp = {'accession': deepcopy(row['accession']), column: deepcopy(item)}
                                    consolidated.append(row_temp)
                            columns[column] = type_column

                    status_bar.update()

                temporary_table = 'temp_consolidated_sample'
                query = f"""create table if not exists {self.__schema}.{temporary_table}(accession text"""
                for column in sorted(columns):
                    query = f"""{query}, "{column}" {columns[column]}"""
                query = f"""{query});"""
                cursor.execute(query)
                self.__connection.commit()

                status_bar.update()

                for row in consolidated:
                    temp_columns = list(row.keys())
                    query = f"""insert into {self.__schema}.{temporary_table} ({', '.join([f'"{column}"' for column in temp_columns])})"""
                    query = f"""{query} values ({', '.join(('%s',) * len(row))})"""
                    values = []
                    for column in temp_columns:
                        if column == 'accession':
                            values.append(row[column])
                        elif columns[column] == 'double precision':
                            values.append(float(row[column]))
                        elif columns[column] == 'bigint':
                            values.append(int(row[column]))
                        else:
                            values.append(row[column])
                    cursor.execute(query, values)
                    status_bar.update()

                self.__connection.commit()

                cursor.execute(f"""drop table {self.__schema}.consolidated_sample;""")
                self.__connection.commit()

                cursor.execute(f"""
                create table {self.__schema}.consolidated_sample as
                select * from {self.__schema}.{temporary_table}
                """)

                cursor.execute(f"""
                drop table {self.__schema}.{temporary_table}
                """)

                self.__connection.commit()

                status_bar.update()

    # create database structure for application.
    def create_annotation_tables(self):
        with self.__connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.__schema};")
            self.__connection.commit()

            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {self.__schema}.table_pair_field(
                internal_id bigserial primary key,
                table_name text not null,
                field_a text not null,
                field_b text not null,
                unique(table_name, field_a, field_b)
            );""")
            self.__connection.commit()

            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {self.__schema}.table_sub_pair_field(
                internal_id bigserial,
                field_sub_a text not null,
                field_sub_b text not null,
                table_pair_field_id bigint not null,
                unique(field_sub_a, field_sub_b, table_pair_field_id),
                foreign key (table_pair_field_id) references {self.__schema}.table_pair_field(internal_id) 
                    on update cascade on delete restrict
            );""")
            self.__connection.commit()

            cursor.execute(f"""CREATE TABLE IF NOT EXISTS {self.__schema}.table_annotation(
                                    schema_name text not null,
                                    table_name text not null,
                                    column_name text not null,
                                    internal_id bigint not null,
                                    tree text 
                                );""")
            self.__connection.commit()

            cursor.execute(f"""create table if not exists {self.__schema}.table_unstructured_field(
                            internal_id bigserial,
                            schema_name text not null,
                            table_name text not null,
                            field text not null
                        );""")
            self.__connection.commit()

            cursor.execute(f"""create table if not exists {self.__schema}.ignored_table(
                            internal_id bigserial,
                            schema_name text not null,
                            table_name text not null,
                            unique (table_name)
                        );""")
            self.__connection.commit()

            cursor.execute(f"""create table if not exists {self.__schema}.ignored_field(
                            internal_id bigserial,
                            schema_name text not null,
                            table_name text not null,
                            field text not null,
                            unique (table_name, field)
                        );""")
            self.__connection.commit()

            cursor.execute(f"""create table if not exists {self.__schema}.table_join_fields(
                            internal_id bigserial,
                            schema_a text not null,
                            table_a text not null,
                            field_a text not null,
                            schema_b text not null,
                            table_b text not null,
                            field_b text not null
                        );""")
            self.__connection.commit()

            cursor.execute(f"""create table if not exists {self.__schema}.ignored_table_join_fields(
                            internal_id bigserial,
                            schema_a text not null,
                            table_a text not null,
                            field_a text not null,
                            schema_b text not null,
                            table_b text not null,
                            field_b text not null
                        );""")
            self.__connection.commit()

    def create_consolidated_mesh_terms_table(self, cache=False):
        consolidated_table_name = 'consolidated_sample_mining_terms'
        consolidated = {}
        with self.__connection.cursor() as cursor:
            query = f"""select 
                    schema_name,
                    table_name,
                    column_name,
                    internal_id,
                    category,
                    mesh_term,
                    term,
                    sample_accession
                    from {self.__schema}.sample_mining_terms
                    """
            if self.__limit_record_number is not None:
                query = f"{query} order by random() limit {self.__limit_record_number}"
            cursor.execute(query)
            columns = [i.name for i in cursor.description]
            for row in self.__unpack_results(cursor):
                row = dict(zip(columns, row))
                if row['sample_accession'] not in consolidated:
                    consolidated[row['sample_accession']] = {}
                if row['column_name'] not in consolidated[row['sample_accession']]:
                    consolidated[row['sample_accession']][row['column_name']] = set()
                consolidated[row['sample_accession']][row['column_name']].add(row['mesh_term'])

            query = f"""select * from {self.__schema}.consolidated_sample"""
            if self.__limit_record_number is not None:
                query = f"{query} order by random() limit {self.__limit_record_number}"
            cursor.execute(query)

            # Normalizando nomes.
            columns = [i.name for i in cursor.description]
            normalized_columns = self.get_mesh_normalized_name(columns=columns, accession_field='accession',
                                                               cache=cache)

            for row in self.__unpack_results(cursor):
                row = dict(zip(columns, row))
                if row['accession'] not in consolidated:
                    consolidated[row['accession']] = {}
                for column, value in row.items():
                    if value is not None and column != 'accession':
                        for normalized_column in normalized_columns[column]:
                            if normalized_column not in consolidated[row['accession']]:
                                consolidated[row['accession']][normalized_column] = set()
                            consolidated[row['accession']][normalized_column].add(value)
            for accession, value in consolidated.items():
                value['accession'] = accession
            consolidated = [value for accession, value in consolidated.items()]
            for row in consolidated:
                temp = deepcopy(row)
                for column, value in temp.items():
                    if isinstance(value, set):
                        for i, item in enumerate(value):
                            if i == 0:
                                row[column] = deepcopy(item)
                            else:
                                row_temp = deepcopy(temp)
                                for j in row_temp:
                                    if j != 'accession':
                                        row_temp[j] = None
                                row_temp[column] = deepcopy(item)
                                consolidated.append(row_temp)

            engine = create_engine(f"postgresql://{self.__parameters['user']}"
                                   f":{self.__parameters['password']}"
                                   f"@{self.__parameters['host']}"
                                   f":{self.__parameters['port']}"
                                   f"/{self.__parameters['database']}")
            dataframe = pd.DataFrame(consolidated)
            dataframe.drop_duplicates(inplace=True)
            dataframe.set_index('accession', inplace=True)
            with engine.begin() as connection:
                dataframe[sorted(dataframe.columns)].to_sql(
                    name=consolidated_table_name,
                    con=connection,
                    schema=self.__schema,
                    if_exists='replace',
                    index=True,
                    index_label='accession',
                    chunksize=self.__chunk_size,
                )

    def create_ignored_fields_tables(self):
        with self.__connection.cursor() as cursor:
            temporary_table_name = 'temp_ignored_field'
            cursor.execute(
                f"""create temporary table if not exists {temporary_table_name} (
                      schema_name text not null,
                      table_name text not null,
                      field text not null
                    );
                """
            )
            with open(path.join(path.dirname(path.realpath(__file__)), 'template', 'ignored_field')) as ignored_file:
                for line in ignored_file:
                    _, schema, table, field = [str(i.strip()) for i in line.split(',')]
                    cursor.execute(f"""
                    insert into {temporary_table_name}(schema_name, table_name, field) values(%s, %s, %s)
                    """, (schema, table, field))
                cursor.execute(f"""
                insert into {self.__schema}.ignored_field(schema_name, table_name, field)
                select tt.schema_name, tt.table_name, tt.field
                from {temporary_table_name} tt
                left join {self.__schema}.ignored_field i
                    on tt.schema_name = i.schema_name
                    and tt.table_name = i.table_name
                    and tt.field = i.field
                where i.schema_name is null and i.table_name is null and i.field is null
                group by tt.schema_name, tt.table_name, tt.field
                """)
                self.__connection.commit()
            temporary_table_name = 'temp_ignored_table'
            cursor.execute(
                f"""create table if not exists {temporary_table_name}
                    (
                        schema_name text not null,
                        table_name  text not null
                    );
                """
            )
            with open(path.join(path.dirname(path.realpath(__file__)), 'template', 'ignored_table')) as ignored_file:
                for line in ignored_file:
                    _, schema, table = [str(i.strip()) for i in line.split(',')]
                    cursor.execute(f"""
                    insert into {temporary_table_name}(schema_name, table_name) values(%s, %s)
                    """, (schema, table))
                cursor.execute(f"""
                insert into {self.__schema}.ignored_table(schema_name, table_name)
                select tt.schema_name, tt.table_name
                from {temporary_table_name} tt
                left join {self.__schema}.ignored_table i
                    on tt.schema_name = i.schema_name
                    and tt.table_name = i.table_name
                where i.schema_name is null and i.table_name is null
                group by tt.schema_name, tt.table_name
                """)
                self.__connection.commit()

    # TODO: incluir tabelas e colunas proibidas como filtro antes de processar os registros
    def create_sample_mining_terms_table(self):
        """Create database consolidated sample mining terms table."""
        consolidated_table_name = f'{self.__schema}.sample_mining_terms'
        query = f"""create table if not exists {consolidated_table_name} (
            schema_name text,
            table_name text,
            column_name text,
            internal_id bigint,
            category text,
            mesh_term text,
            term text,
            sample_accession text
        );"""
        with self.__connection.cursor() as cursor:
            cursor.execute(query)
            self.__connection.commit()

            temp_table_name = 'temp_consolidated_sample_mining_terms'
            query = f"""create temporary table if not exists {temp_table_name} (
                schema_name text,
                table_name text,
                column_name text,
                internal_id bigint,
                category text,
                mesh_term text,
                term text,
                sample_accession text
            );"""
            cursor.execute(query)

            query = f"""insert into {temp_table_name} (
                schema_name, table_name, column_name, internal_id, category, mesh_term, term, sample_accession)
                values (%s, %s, %s, %s, %s, %s, %s, %s);"""
            for term in self.get_mining_terms():
                cursor.execute(query, (
                    term['schema_name'], term['table_name'], term['column_name'], term['internal_id'], term['category'],
                    term['mesh_term'], term['term'], term['sample_accession']))

            query = f"""insert into {consolidated_table_name}
                    (schema_name, table_name, column_name, internal_id, category, mesh_term, term, sample_accession)
                select
                    t.schema_name, t.table_name, t.column_name, t.internal_id, t.category, t.mesh_term, t.term, t.sample_accession
                from {temp_table_name} t
                left join {consolidated_table_name} c on c.schema_name = t.schema_name
                    and c.table_name = t.table_name
                    and c.column_name = t.column_name
                    and c.internal_id = t.internal_id
                    and c.category = t.category
                    and c.mesh_term = t.mesh_term
                    and c.term = t.term
                    and c.sample_accession = t.sample_accession
                where c.schema_name is null and c.table_name is null and c.column_name is null and c.internal_id is null
                    and c.category is null and c.mesh_term is null and c.term is null and c.sample_accession is null
            """
            cursor.execute(query)
            self.__connection.commit()

    def create_special_consolidate_sample(self):
        # colunas
        table_columns = {}
        consolidated = {}
        for table, value in self.get_tables_columns(data_mining_tables=True).items():
            schema = value['schema']
            table_name = value['name']
            columns = value['columns']
            if schema not in table_columns:
                table_columns[schema] = {table: columns}
            else:
                table_columns[schema][table] = columns
        ignored = self.get_ignored_fields_with_schema()
        for item in ignored['tables']:
            schema = item['schema']
            table = item['table']
            if schema in table_columns:
                if table in table_columns[schema]:
                    del table_columns[schema][table]
        for item in ignored['columns']:
            schema = item['schema']
            table = item['table']
            column = item['column']
            if schema in table_columns:
                if table in table_columns[schema]:
                    if column in table_columns[schema][table]:
                        table_columns[schema][table].remove(column)
                    if len(table_columns[schema][table]) == 0:
                        del table_columns[schema][table]
        # substances
        with self.__connection.cursor() as cursor:
            cursor.execute(f"""
            create table if not exists {self.__schema}.drug_name (
                name text
            )
            """)

            temporary_table = 'temp_drug_name'
            cursor.execute(f"""create temporary table {temporary_table} (
                            name text
                        )""")

            with open(path.join(path.dirname(path.realpath(__file__)), 'template', 'drug_name')) as drug_file:
                for drug_name in drug_file:
                    drug_name = drug_name.strip()
                    if len(drug_name) > 0:
                        cursor.execute(f"""
                        insert into {temporary_table} (name) values (%s)
                        """, (drug_name, ))
                self.__connection.commit()

                cursor.execute(f"""
                insert into {self.__schema}.drug_name
                select tt.name 
                from {temporary_table} tt
                left join {self.__schema}.drug_name dn
                    on tt.name ilike dn.name
                where dn.name is null
                """)
                self.__connection.commit()

            drugs = set()

            temporary_table = 'temp_consolidated_sample_drug'
            cursor.execute(f"""create temporary table {temporary_table} (
                sample_accession text,
                drug text
            )""")

            cursor.execute(f"""select name from {self.__schema}.drug_name""")
            for drug_name, in self.__unpack_results(cursor):
                drugs.add(drug_name.lower())

            drug_columns = {}
            other_columns = {}
            for schema in table_columns:
                for table in table_columns[schema]:
                    for column in table_columns[schema][table]:
                        is_drug = False
                        drug = None
                        # coluna é a droga
                        if column in drugs:
                            is_drug = True
                            drug = column
                        else:
                            # coluna possui a droga
                            # TODO: verificar nomes pequenos, principalmente de drogas não aprovadas
                            for item in column.split('_'):
                                if item in drugs:
                                    is_drug = True
                                    drug = item
                                    break
                        if is_drug:
                            if schema not in drug_columns:
                                drug_columns = {schema: {table: [{'column': column, 'drug': drug}]}}
                            elif table not in drug_columns[schema]:
                                drug_columns[schema][table] = [{'column': column, 'drug': drug}]
                            else:
                                drug_columns[schema][table].append({'column': column, 'drug': drug})
                        else:
                            if schema not in other_columns:
                                other_columns = {schema: {table: {column}}}
                            elif table not in other_columns[schema]:
                                other_columns[schema][table] = {column}
                            else:
                                other_columns[schema][table].add(column)
            size = 0
            for schema in drug_columns:
                for table in drug_columns[schema]:
                    size += len(table_columns[schema][table])
            with StatusBar('Searching for samples in drug columns...', size) as status_bar:
                with self.__connection.cursor() as cursor_2:
                    for schema in drug_columns:
                        for table in drug_columns[schema]:
                            for item in drug_columns[schema][table]:
                                column = item['column']
                                drug = item['drug']
                                if schema == 'public':
                                    rows = self.get_table_related_data(
                                        table=table,
                                        reference_table='experiment_package_sample',
                                        table_columns_filter={column}
                                    )
                                else:
                                    rows = self.get_mining_related_data(
                                        consolidated_table=table,
                                        referenced_table='experiment_package_sample',
                                        table_columns_filter={column}
                                    )
                                for row in rows:
                                    value = row[f'{table}_{column}']
                                    if value is not None:
                                        value = value.strip()
                                        if value not in NONE_VALUES:
                                            accession = row['experiment_package_sample_accession']
                                            cursor_2.execute(f"""
                                            insert into {temporary_table} (sample_accession, drug)
                                            values (%s, %s)
                                            """, (accession, drug))
                            status_bar.update()

            # procurando drogas em substâncias mapeadas
            cursor.execute(f"""
            select sample_accession, mesh_term, term from {self.__schema}.sample_mining_terms where category = 'name of substance'
            """)

            with StatusBar('Searching for drugs in MeSh terms...', cursor.rowcount) as status_bar:
                with self.__connection.cursor() as cursor_2:
                    for accession, mesh_term, term in self.__unpack_results(cursor):
                        if mesh_term in drugs:
                            cursor_2.execute(f"""
                            insert into {temporary_table} (sample_accession, drug)
                            values (%s, %s)
                            """, (accession, mesh_term))
                        elif term in drugs:
                            cursor_2.execute(f"""
                            insert into {temporary_table} (sample_accession, drug)
                            values (%s, %s)
                            """, (accession, term))
                status_bar.update()

            # procurando drogas em todas as tabelas
            size = 0
            for schema in other_columns:
                for table in other_columns[schema]:
                    size += len(other_columns[schema][table])
            with StatusBar('Searching for drugs in all fields...', size) as status_bar:
                with self.__connection.cursor() as cursor_2:
                    for schema in other_columns:
                        for table in other_columns[schema]:
                            for column in other_columns[schema][table]:
                                if schema == 'public':
                                    rows = self.get_table_related_data(
                                        table=table,
                                        reference_table='experiment_package_sample',
                                        table_columns_filter={column}
                                    )
                                else:
                                    rows = self.get_mining_related_data(
                                        consolidated_table=table,
                                        referenced_table='experiment_package_sample',
                                        table_columns_filter={column}
                                    )
                                for row in rows:
                                    value = row[f'{table}_{column}']
                                    if value is not None:
                                        if not isinstance(value, int):
                                            value = value.strip().lower()
                                            if value not in NONE_VALUES:
                                                is_drug = False
                                                drug = set()
                                                if value in drugs:
                                                    is_drug = True
                                                    drug.add(value)
                                                else:
                                                    # coluna possui a droga
                                                    # TODO: verificar nomes pequenos, principalmente de drogas não aprovadas
                                                    for item in value.split(' '):
                                                        if item in drugs:
                                                            is_drug = True
                                                            drug.add(item)
                                                    for item in drugs:
                                                        if item in value:
                                                            is_drug = True
                                                            drug.add(item)
                                                if is_drug:
                                                    for d in drug:
                                                        accession = row['experiment_package_sample_accession']
                                                        cursor_2.execute(f"""
                                                        insert into {temporary_table} (sample_accession, drug)
                                                        values (%s, %s)
                                                        """, (accession, d))
                                status_bar.update()

            print('Persisting data...')
            cursor.execute(f"""
            create table if not exists {self.__schema}.consolidated_drug_sample (
                accession text,
                drug text
            )
            """)
            cursor.execute(f"""
            insert into {self.__schema}.consolidated_drug_sample (accession, drug)
            select d.accession, d.drug 
            from (select sample_accession as accession, drug from {temporary_table} group by sample_accession, drug) as d
            left join {self.__schema}.consolidated_drug_sample cds on d.accession = cds.accession and d.drug = cds.drug
            where cds.accession is null and cds.drug is null
            """)
            self.__connection.commit()

    # create database structure from xml
    def create_from_xml(self, xml, create_structure=True):
        data = etree.parse(xml, etree.XMLParser(remove_blank_text=True, remove_comments=True))
        entities = {}
        references = {}
        paths = {}
        logging.info('Analyzing XML structure...')
        start_node = 'EXPERIMENT_PACKAGE'
        XML.generate_structure(data.getroot(), entities, references, paths, start_node=start_node)
        logging.info('Creating database structure...')
        with StatusBar('Creating database structure', len(entities) * 2) as status_bar:
            if create_structure:
                bar_position = 0
                status_bar.update()
                with self.__connection.cursor() as cursor:
                    query = f"""CREATE TABLE table_name(
                        id text primary key,
                        name text,
                        reference text
                    );"""
                    cursor.execute(query)
                    # TODO: inserir a tabela raiz na tabela de metadados (table_name)
                    for entity in entities:
                        query = f'CREATE TABLE "{self.get_short_name(entity)}"('
                        query += 'internal_id BIGINT PRIMARY KEY'
                        for i, field in enumerate(entities[entity]):
                            if field != 'internal_id':
                                query += f', {field} {entities[entity][field]["type"]}'
                        query += f""");"""
                        cursor.execute(query)
                        bar_position += 1
                        status_bar.update()
                    self.__connection.commit()
                    for entity in references:
                        query = f'ALTER TABLE "{self.get_short_name(entity)}"'
                        query += f' ADD COLUMN {self.get_short_name(references[entity])}_id BIGINT;'
                        cursor.execute(query)
                        self.__connection.commit()
                        query = f'ALTER TABLE "{self.get_short_name(entity)}"'
                        query += f' ADD FOREIGN KEY ({self.get_short_name(references[entity])}_id)'
                        query += f' REFERENCES "{self.get_short_name(references[entity])}"(internal_id)'
                        query += f' ON UPDATE CASCADE ON DELETE RESTRICT;'
                        cursor.execute(query)
                        cursor.execute(
                            f"""INSERT INTO table_name(id, name, reference) VALUES (%s, %s, %s)""",
                            (self.get_short_name(entity), entity, self.get_short_name(references[entity]))
                        )
                        self.__connection.commit()
                        bar_position += 1
                        status_bar.update()
        logging.info('Inserting database data...')
        self.insert_from_xml(data.getroot(), entities, references, paths, start_node=start_node)

    def detect_data(self, cache: bool = False):
        logging.info('Search for unstructured data...')
        if cache:
            if self.mining is None:
                self.mining = Mining()
        else:
            self.mining = Mining()
        tables = self.get_tables_columns(show_type=True, data_mining_tables=True)

        with StatusBar('Searching for data types', len(tables)) as status_bar:
            table_pair_categorical_fields = {}
            with self.__connection.cursor() as cursor:
                cursor.execute(f"""select table_name, field_a from {self.__schema}.table_pair_field;""")
                for table_name, field_a in self.__unpack_results(cursor):
                    if table_name not in table_pair_categorical_fields:
                        table_pair_categorical_fields[table_name] = set()
                    table_pair_categorical_fields[table_name].add(field_a)
            unstructured_tables = {}
            for table in sorted(tables):
                unstructured_columns = []
                for column in tables[table]['columns']:
                    if not column['structured']:
                        if table in table_pair_categorical_fields:
                            if column['name'] in table_pair_categorical_fields[table]:
                                continue
                        unstructured_columns.append(column['name'])
                if len(unstructured_columns):
                    unstructured_tables[table] = {'columns': unstructured_columns, 'schema': tables[table]['schema']}
                self.insert_table_unstructured_fields(tables[table]['schema'], table, unstructured_columns)
                status_bar.update()

        # TODO: rever esta estratégia de sobrescrever a função. Pode ser perigoso.
        global get_pos_tag

        def get_pos_tag(_text, _internal_id, _mining):
            temp = _mining.get_pos_tag(_text, entity=True)
            return _internal_id, temp

        with StatusBar('Processing unstructured data', len(unstructured_tables)) as status_bar:
            with self.__connection.cursor() as cursor, self.__connection_2.cursor() as cursor_2:
                cursor_2.execute(f"""
                create temporary table temp_table_annotation
                (
                    schema_name text not null,
                    table_name  text not null,
                    column_name text not null,
                    internal_id bigint       not null,
                    tree        text
                );

                """)
                self.__connection_2.commit()
                for table in unstructured_tables:
                    if unstructured_tables[table]['schema'] == 'public':
                        for column in unstructured_tables[table]['columns']:
                            annotated_table = f"{self.__schema}.table_annotation"
                            primary_key_name = 'internal_id'
                            query = f"""
                                select {primary_key_name}, {column}
                                from {unstructured_tables[table]['schema']}.{table}
                                where {column} is not null"""
                            if self.__limit_record_number:
                                query += f" order by random() limit {self.__limit_record_number}"
                            cursor.execute(query)
                            detect_count = 0
                            with NamedTemporaryFile() as tempfile:
                                sep = '<===>'
                                with open(tempfile.name, 'w') as file:
                                    for record in self.__unpack_results(cursor):
                                        new_line = '\n'
                                        file.write(
                                            f'{sep.join([str(i).replace(new_line, " ").strip() for i in record])}\n')
                                with (ThreadPoolExecutor(max_workers=cpu_count()) as executor):
                                    futures = []
                                    with open(tempfile.name, 'r') as file:
                                        for record in file:
                                            internal_id, text = record.split(sep)
                                            internal_id = int(internal_id)
                                            future = executor.submit(get_pos_tag, text, internal_id, self.mining)
                                            futures.append(future)
                                        errs = []
                                        for future in as_completed(futures):
                                            if future.exception():
                                                errs.append(str(future.exception()))
                                            else:
                                                internal_id, tree = future.result()
                                                save = False
                                                for t in tree:
                                                    if save:
                                                        break
                                                    for j in t:
                                                        if isinstance(j, Tree):
                                                            save = True
                                                            break
                                                if save:
                                                    cursor_2.execute(
                                                        f"""
                                                                    insert into temp_table_annotation(schema_name, table_name, column_name, internal_id, tree)
                                                                    values(%s, %s, %s, %s, %s);""",
                                                        (unstructured_tables[table]['schema'], table, column,
                                                         internal_id, str(tree))
                                                    )
                                                    detect_count += 1
                                        if len(errs) > 0:
                                            errs = '\n'.join(errs)
                                            logging.error(f'{len(errs)} errs detected on processing table {table}.')
                                            logging.error(f'{errs}')
                                        if detect_count > 0:
                                            self.__connection.commit()
                            cursor_2.execute(f"""
                            insert into {annotated_table}(schema_name, table_name, column_name, internal_id, tree)
                            select tt.schema_name, tt.table_name, tt.column_name, tt.internal_id, tt.tree
                            from temp_table_annotation tt
                            left join {annotated_table} at 
                                on tt.schema_name = at.schema_name
                                and tt.table_name = at.table_name
                                and tt.column_name = at.column_name
                                and tt.internal_id = at.internal_id
                                and tt.tree = at.tree
                            where 
                                at.schema_name is null
                                and at.table_name is null
                                and at.column_name is null
                                and at.internal_id is null
                                and at.tree is null
                            """)
                            self.__connection_2.commit()
                    status_bar.update()

    def extract_groups(self, output_dir):
        if self.mining is None:
            self.mining = Mining()

        logging.info('Checking database strucutures...')
        ignored_tables = self.get_ignored_fields()
        # Ignorando campos pares uma vez que serão processados separadamente mais abaixo.
        for column in self.get_pair_fields():
            ignored_tables['columns'].append(column)
        tables = self.get_tables_columns(show_type=True, data_mining_tables=True)
        # vertor com campos a serem processados.
        work_fields = []
        Field = namedtuple('Field', 'schema table column')
        for table in tables:
            if table in ignored_tables['tables']:
                continue
            if tables[table]['schema'] == self.__schema:
                continue
            tables[table]['references'] = self.get_table_references(table=table)
            for column in tables[table]['columns']:
                for temp in ignored_tables['columns']:
                    if table == temp['table'] and column['name'] == temp['column']:
                        break
                else:
                    if (
                            column['name'] != 'internal_id'
                            and (  # Colunas apenas de ligação são ignoradas.
                            len(tables[table]['references']) > 0
                            and column['name'] != tables[table]['references'][0]['origin key']
                    )
                    ):
                        field = Field(schema=tables[table]['schema'], table=table, column=column)
                        work_fields.append(field)

        logging.info(f'Processing attributes...')
        dataframe = {}
        for field in work_fields:
            result_field_name = f"{field.table}__{field.column['name']}"
            dataframe[result_field_name] = []
        dataframe = pd.DataFrame(dataframe)
        dataframe = dataframe.astype(str)
        logging.info(f'Processing structured fields...')
        with StatusBar(task_name=f'Working on structured fields', size=len(work_fields)) as status_bar:
            for field in work_fields:
                if field.column['structured'] is True:
                    column_name = f"{field.table}_{field.column['name']}"
                    result_field_name = format_name(f"{field.table}__{field.column['name']}")
                    for record in self.get_table_related_data(table=field.table,
                                                              reference_table='experiment_package_sample'):
                        value = record[column_name]
                        sample = record['experiment_package_sample_accession']
                        if value is not None:
                            dataframe.at[sample, result_field_name] = value
                status_bar.update()

        logging.info(f'Writing csv file...')
        dataframe.to_csv(path.join(output_dir, f'sample_caracteristics.csv'))

        logging.info(f'Processing pair attributes...')
        # TODO: incluir esses resultados no banco de dados
        # TODO: incluir outros consolidados no processamento
        with self.__connection.cursor() as cursor:
            query = f"""select * from {self.__schema}.consolidated_sample"""  # TODO: incluir outras tabelas consolidadas, recuperar accession
            if self.__limit_record_number:
                query += f" order by random() limit {self.__limit_record_number}"
            cursor.execute(query)
            columns = [i.name for i in cursor.description]
            temp = [i.replace('_', ' ') for i in columns]

            def get_synonyms(_query, _subject):
                return self.mining.get_synonyms(column, temp, cache=True)

            group_columns = {}
            for column in columns:
                synonyms = get_synonyms(column, temp)

                for i in synonyms:
                    key = None
                    for synonym in synonyms[i]:
                        if synonym in group_columns:
                            key = synonym
                            break
                    if key is None:
                        key = i
                    if key not in group_columns:
                        group_columns[key] = set()
                    for synonym in synonyms[i]:
                        if synonym in temp and synonym != i and synonym in columns:
                            group_columns[key].add(synonym)
                    if len(group_columns[key]) == 0:
                        del group_columns[key]
                    else:
                        print(group_columns)
            temp = {}
            for column in columns:
                temp[column] = {}
            temp = pd.DataFrame(temp)
            temp = temp.astype(str)
            dataframe = pd.concat((dataframe, temp), axis=1)
            del temp
            with StatusBar(task_name=f'Working on pair fields',
                           size=cursor.rowcount) as status_bar:  # TODO: implementar tag values já processados no banco
                for record in self.__unpack_results(cursor):
                    record = dict(zip(columns, record))
                    for column in record:
                        if column != 'accession' and column != 'internal_id':
                            if record[column] is not None:
                                dataframe.at[record['accession'], column] = str(record[column])
                    status_bar.update()

        logging.info(f'Writing csv file...')
        dataframe.to_csv(path.join(output_dir, f'sample_caracteristics.csv'))

        logging.info(f'Processing sub pair attributes...')

        def get_field_name(_sub_pair_field):
            _table_name = self.get_short_name(
                '_'.join([
                    _sub_pair_field['table'],
                    _sub_pair_field['field_a'],
                    _sub_pair_field['field_sub_a'],
                    _sub_pair_field['field_sub_b']])
            ).strip().replace('"', '').replace("'", '').replace(' ', '_').lower()
            for i in '()[]?-.,!+':
                _table_name = _table_name.replace(i, '_')
            _table_name = _table_name.replace('>=', '_more_or_equal_than_')
            _table_name = _table_name.replace('>', '_more_than_')
            _table_name = _table_name.replace('<=', '_less_or_equal_than_')
            _table_name = _table_name.replace('<', '_less_than_')
            _table_name = _table_name.replace('%', '_percent_')
            while '__' in _table_name:
                _table_name = _table_name.replace('__', '_')
            if _table_name.endswith('_'):
                _table_name = _table_name[:-1]
            return _table_name

        sub_pair_fields = self.get_sub_pair_fields()

        fields = set()
        sub_dataframes = {}
        for sub_pair_field in sub_pair_fields:
            fields.add(get_field_name(sub_pair_field))
            if sub_pair_field['table'] not in sub_dataframes:
                sub_dataframes[sub_pair_field['table']] = pd.DataFrame()
        with StatusBar('Working on sub par fields...', size=len(fields)) as status_bar:
            for field in fields:
                for sub_pair_field in sub_pair_fields:
                    if field == get_field_name(sub_pair_field):
                        name = format_name(' '.join((sub_pair_field['field_sub_a'], sub_pair_field['field_sub_b'])))
                        if name not in sub_dataframes[sub_pair_field['table']]:
                            temp = {name: {}}
                            temp = pd.DataFrame(temp)
                            temp = temp.astype(str)
                            sub_dataframes[sub_pair_field['table']] = pd.concat(
                                (sub_dataframes[sub_pair_field['table']], temp), axis=1)
                        query = f"""
                        select internal_id, {sub_pair_field['field_b']}
                        from {sub_pair_field['table']}
                        where {sub_pair_field['field_a']} ilike %s and {sub_pair_field['field_b']} is not null
                        """
                        if self.__limit_record_number:
                            query += f" order by random() limit {self.__limit_record_number}"
                        parameters = (f"{sub_pair_field['field_sub_a']}%{sub_pair_field['field_sub_b']}",)
                        with self.__connection_2.cursor() as cursor_2:
                            cursor_2.execute(query, parameters)
                            for internal_id, value, in self.__unpack_results(cursor_2):
                                related_data = self.get_table_related_data(
                                    table=sub_pair_field['table'],
                                    reference_table='experiment_package_sample',
                                    table_internal_id=internal_id
                                )
                                accession = None
                                for i in related_data:
                                    if accession is None:
                                        accession = i['experiment_package_sample_accession']
                                    elif accession != i['experiment_package_sample_accession']:
                                        logging.warning(f"More then one accession detected for record {accession}."
                                                        f" New: {i['experiment_package_sample_accession']}")
                                sub_dataframes[sub_pair_field['table']].at[accession, name] = value
                status_bar.update()

        logging.info(f'Writing csv file...')
        for name in sub_dataframes:
            sub_dataframes[name].to_csv(
                path.join(output_dir, f'sample_caracteristics_{name}.csv'), index_label='sample_accession')
            dataframe = pd.concat((dataframe, sub_dataframes[name].add_prefix(name + '__')), axis=1)
        dataframe.to_csv(path.join(output_dir, f'sample_caracteristics.csv'), index_label='sample_accession')

        logging.info(f'Processing unstructured fields...')
        with self.__connection_2.cursor() as cursor_2:
            query = f"""
            select schema_name, table_name, column_name, internal_id, tree
            from {self.__schema}.table_annotation
            """
            if self.__limit_record_number:
                query += f' order by random() limit {self.__limit_record_number}'
            cursor_2.execute(query)

            with StatusBar(task_name=f'Working on unstructured fields', size=cursor_2.rowcount) as status_bar:
                for schema_name, table_name, column_name, internal_id, tree in self.__unpack_results(cursor_2):
                    if schema_name != self.__schema:
                        related_data = self.get_table_related_data(
                            table=table_name,
                            reference_table='experiment_package_sample',
                            table_internal_id=internal_id
                        )
                        accession = None
                        for i in related_data:
                            if accession is None:
                                accession = i['experiment_package_sample_accession']
                            elif accession != i['experiment_package_sample_accession']:
                                logging.warning(f"More then one accession detected for record {accession}."
                                                f" New: {i['experiment_package_sample_accession']}")
                    else:
                        query = f"""
                        select accession
                        from {schema_name}.{table_name}
                        where internal_id = %s
                        """
                        cursor_2.execute(query, (internal_id,))
                        accession, = cursor_2.fetchone()
                    trees = eval(tree)
                    for i, value in enumerate(trees):  # TODO: pensar na importância de se usar a posição da frase
                        for entity in Mining.get_entities_from_tree(value):
                            column = (f'{entity.label.replace(" ", "_")}_{column_name}__'
                                      f'{format_name(" ".join([i[0] for i in entity.leaves]))}')
                            if column not in dataframe:
                                temp = {column: {}}
                                temp = pd.DataFrame(temp)
                                temp = temp.astype(str)
                                dataframe = pd.concat((dataframe, temp), axis=1)
                            dataframe.at[accession, column] = True
                    status_bar.update()

        logging.info(f'Writing csv file...')

        dataframe = dataframe.replace(r'^\s*$', None, regex=True)
        dataframe = dataframe.replace(r'^na$', None, regex=True)
        dataframe = dataframe.replace(r'^none$', None, regex=True)
        dataframe = dataframe.replace(r'^null$', None, regex=True)
        dataframe = dataframe.dropna(axis=1, how='all')

        dataframe.to_csv(path.join(output_dir, f'sample_caracteristics.csv'), index_label='sample_accession')

        # TODO: implementar nova etapa de agrupaento de sinônimos.

        logging.info(f'Testing groups...')
        # Testando agrupamentos
        dataframe = pd.read_csv(path.join(output_dir, f'sample_caracteristics.csv'), low_memory=False)
        dataframe = dataframe.astype(str)
        # treating null and empty values
        dataframe = dataframe.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        dataframe = dataframe.fillna('')

        # Elbow curve
        i = len(dataframe) - 1
        k = range(1, i)
        cost = []
        with StatusBar('Testing groups...', size=i) as status_bar:
            for num_clusters in list(k):
                kmode = KModes(n_clusters=num_clusters, init="random", n_init=30, n_jobs=-1, random_state=42)
                kmode.fit_predict(dataframe)
                cost.append(kmode.cost_)

                x = [i for i in k]
                if len(x) > len(cost):
                    x = x[:len(cost)]

                plt.plot(x, cost, 'bx-')
                plt.xlabel('Number of clusters')
                plt.ylabel('Cost')
                plt.title('Optimization curve. Cost x Cluster')
                if path.exists(path.join(output_dir, f'cost_vs_cluster.png')):
                    copyfile(path.join(output_dir, f'cost_vs_cluster.png'),
                             path.join(output_dir, f'cost_vs_cluster_previous_step.png'))
                plt.savefig(path.join(output_dir, f'cost_vs_cluster.png'))

                status_bar.update()

        pd.DataFrame(data={'cluster number': x, 'cost': cost}).to_csv(
            path.join(output_dir, f'sample_caracteristics_groups_costs.csv'), index=False)
        kn = KneeLocator(k, cost, curve='convex', direction='decreasing')
        logging.info(f'Best select cluster numbers: {kn.knee}.')

        logging.info(f'Making groups...')
        if kn.knee:
            kmode = KModes(n_clusters=kn.knee, init="random", n_init=30, random_state=42)
            clusters = kmode.fit_predict(dataframe)
            dataframe.insert(0, 'cluster', clusters)
            dataframe.to_csv(path.join(output_dir, f'sample_caracteristics_groups.csv'), index=False)

    def get_database_name(self):
        return self.__database

    def get_ignored_fields(self):
        with self.__connection_2.cursor() as cursor_2:
            cursor_2.execute(f"""select table_name from {self.__schema}.ignored_table;""")
            tables = [table_name for table_name, in self.__unpack_results(cursor_2)]
            cursor_2.execute(f"""select table_name, field from {self.__schema}.ignored_field""")
            columns = [dict(table=table_name, column=column) for table_name, column in self.__unpack_results(cursor_2)]
            return dict(tables=tables, columns=columns)

    def get_ignored_fields_with_schema(self):
        with self.__connection_2.cursor() as cursor_2:
            cursor_2.execute(f"""select schema_name, table_name from {self.__schema}.ignored_table;""")
            tables = [dict(schema=schema, table=table_name) for schema, table_name, in self.__unpack_results(cursor_2)]
            cursor_2.execute(f"""select schema_name, table_name, field from {self.__schema}.ignored_field""")
            columns = [dict(schema=schema, table=table_name, column=column) for schema, table_name, column in
                       self.__unpack_results(cursor_2)]
            return dict(tables=tables, columns=columns)

    def get_ignored_table_join_fields(self, schema, table):
        with self.__connection.cursor() as cursor:
        
            cursor.execute(f"""
            select
                schema_a,
                table_a,
                field_a,
                schema_b,
                table_b,
                field_b
            from {self.__schema}.ignored_table_join_fields
            where (schema_a = %s and table_a = %s)
            or (schema_b = %s and table_b = %s)
            """, [schema, table, schema, table])
            
            results = {'combinations': {}, 'keys': {}}
            for schema_a, table_a, field_a, schema_b, table_b, field_b in self.__unpack_results(cursor):
                combinations = {field_a, field_b}
                if field_a in results['combinations']:
                    combinations = combinations | results['combinations'][field_a]
                if field_b in results['combinations']:
                    combinations = combinations | results['combinations'][field_b]
                for field_c in deepcopy(combinations):
                    if field_c != field_a and field_c != field_b:
                        combinations = combinations | results['combinations'][field_c]
                        results['combinations'][field_c] = combinations
                results['combinations'][field_a] = combinations
                results['combinations'][field_b] = combinations

            for key in sorted(deepcopy(results['combinations'])):
                if key in results['combinations']:
                    others = results['combinations'][key] - {key}
                    for other in others:
                        results['keys'][other] = key
                        if other in results['combinations']:
                            del results['combinations'][other]
            return results

    def get_table_join_fields(self, schema, table):
        with self.__connection.cursor() as cursor:

            cursor.execute(f"""
            select
                schema_a,
                table_a,
                field_a,
                schema_b,
                table_b,
                field_b
            from {self.__schema}.table_join_fields
            where (schema_a = %s and table_a = %s)
            or (schema_b = %s and table_b = %s)
            """, [schema, table, schema, table])

            results = {'combinations': {}, 'keys': {}}
            for schema_a, table_a, field_a, schema_b, table_b, field_b in self.__unpack_results(cursor):
                combinations = {field_a, field_b}
                if field_a in results['combinations']:
                    combinations = combinations | results['combinations'][field_a]
                if field_b in results['combinations']:
                    combinations = combinations | results['combinations'][field_b]
                for field_c in deepcopy(combinations):
                    if field_c != field_a and field_c != field_b:
                        combinations = combinations | results['combinations'][field_c]
                        results['combinations'][field_c] = combinations
                results['combinations'][field_a] = combinations
                results['combinations'][field_b] = combinations

            for key in sorted(deepcopy(results['combinations'])):
                if key in results['combinations']:
                    others = results['combinations'][key] - {key}
                    for other in others:
                        results['keys'][other] = key
                        if other in results['combinations']:
                            del results['combinations'][other]
            return results

    def get_mesh_normalized_name(self, columns: list | set, accession_field: str = 'accession', cache: bool = False):
        """Percorre os nomes de colunas da tabela consolidada de amostra e formata usando termos do MeSH.
        Caso encontre correspondências parciais, troca apenas a região correspondente.
        Não havendo correspondências mantém o nome original.

        :param columns: Lista de colunas da tabela consolidada.
        :param accession_field: Nome do campo de identificação. Esse campo é ignorado durante o processo.
        :param cache: Utilizar cache de mineração.

        :return: Dicionário contento um conjunto de nomes possíveis para cada coluna normalizada pelo MeSH."""

        if cache:
            if self.mining is None:
                self.mining = Mining()
        mining = self.mining if cache else Mining()

        columns = {i: {i} for i in columns if i != accession_field}
        mesh_columns = {
            column: mining.mesh.find_term_and_category(column.replace('_', ' '), use_ngrams=True, cut_words=False) for
            column in columns
        }
        for column in mesh_columns:
            if mesh_columns[column]:
                columns[column] = set()
                for mesh_term in mesh_columns[column]:
                    mesh_term['mesh_term'] = mesh_term['mesh_term'].lower()
                    columns[column].add(format_name(mesh_term['mesh_term']))
        return columns

    def get_mining_terms(self):
        """Recupera as anotações por amostras mineradas de texto não estruturado."""
        with self.__connection.cursor() as cursor, self.__connection.cursor() as cursor_2:
            # criando tabela temporária para armazenar dados
            reference_table = 'experiment_package_sample'
            temporary_table = f'temp_mesh_terms'

            cursor.execute(f"""create temporary table {temporary_table}_ids (
                schema_name text,
                table_name text,
                column_name text,
                internal_id bigint,
                sample_accession text
            )""")

            cursor.execute(f"""create temporary table {temporary_table} (
                schema_name text,
                table_name text,
                column_name text,
                internal_id bigint,
                category text,
                term text,
                sample_accession text
            )""")

            query = f"""
            select schema_name, table_name, column_name, internal_id, tree
            from {self.__schema}.table_annotation
            """
            if self.__limit_record_number:
                query += f' order by random() limit {self.__limit_record_number}'

            cursor.execute(query)

            with StatusBar('Querying Mesh term trees...', cursor.rowcount) as status_bar:
                for schema_name, table_name, column_name, internal_id, trees in self.__unpack_results(cursor):
                    trees = eval(trees)
                    for tree in trees:
                        for entity in Mining.get_entities_from_tree(tree):
                            term = ' '.join([i[0] for i in entity.leaves])
                            cursor_2.execute(f"""insert into {temporary_table} (
                            schema_name, table_name, column_name, internal_id, category, term) values (%s, %s, %s, %s, %s, %s);""",
                                             (
                                                 schema_name,
                                                 table_name,
                                                 column_name,
                                                 internal_id,
                                                 entity.label.lower(),
                                                 term.lower()))
                    status_bar.update()
            cursor_2.execute(f"""select 
                schema_name, table_name, column_name, array_agg(distinct(internal_id)) as internal_ids 
            from {temporary_table}
            group by schema_name, table_name, column_name;""")
            with StatusBar('Recovering accession numbers...', cursor_2.rowcount) as status_bar:
                query = f"""insert into {temporary_table}_ids (schema_name, table_name, column_name, internal_id, 
                sample_accession) values (%s, %s, %s, %s, %s);"""
                for schema_name, table_name, column_name, internal_ids in self.__unpack_results(cursor_2):
                    for sample in self.get_table_related_data(
                            table=table_name, table_internal_id=internal_ids,
                            reference_table=reference_table):
                        cursor.execute(query, (
                            schema_name,
                            table_name,
                            column_name,
                            sample[f'{table_name}_internal_id'],
                            sample[f'{reference_table}_accession']
                        ))
                    status_bar.update()

            with StatusBar('Writing accession numbers...', 1) as status_bar:
                cursor.execute(f"""
                update {temporary_table}
                set sample_accession = accessions.sample_accession
                from (
                    select 
                        schema_name, table_name, column_name, internal_id, sample_accession
                    from {temporary_table}_ids
                    group by schema_name, table_name, column_name, internal_id, sample_accession
                ) as accessions
                where {temporary_table}.schema_name = accessions.schema_name 
                    and {temporary_table}.table_name = accessions.table_name
                    and {temporary_table}.column_name = accessions.column_name 
                    and {temporary_table}.internal_id = accessions.internal_id
                """)
                status_bar.update()

            cursor.execute(f"""select schema_name, table_name, column_name, internal_id, category, term, 
            sample_accession from {temporary_table};""")
            with StatusBar('Recovering Mesh Terms...', cursor.rowcount) as status_bar:
                for (schema_name, table_name, column_name, internal_id, category, term,
                     sample_accession) in self.__unpack_results(cursor):
                    category = [i.strip() for i in category.split(':')]
                    mesh_term = category[1]
                    category = category[0]
                    yield dict(schema_name=schema_name, table_name=table_name, column_name=column_name,
                               internal_id=internal_id, category=category, mesh_term=mesh_term, term=term,
                               sample_accession=sample_accession)
                    status_bar.update()

    def get_mining_related_data(self, consolidated_table, referenced_table, table_columns_filter: list | set = None,
                                max_rows: int = None):  # TODO: verificar max_rows. Não está implementado.
        with self.__connection.cursor() as cursor:
            reference = self.mining_references[consolidated_table]['reference_table']
            if table_columns_filter is not None:
                consolidated_table_columns_filter_names = {str(i): value for i, value in
                                                           enumerate(table_columns_filter)}
            columns = (
                ', '.join(
                    [f'{consolidated_table}."{consolidated_table_columns_filter_names[i]}" as "{i}"' for i, value in
                     consolidated_table_columns_filter_names.items()])
                if table_columns_filter else f'{consolidated_table}.*')
            query = (
                f"select {reference}.internal_id, {columns}"
                f" from public.{reference} as {reference}"
                f" join {self.__schema}.{consolidated_table} as {consolidated_table}"
                f" on {consolidated_table}.{self.mining_references[consolidated_table]['column']}"
                f" = {reference}.{self.mining_references[consolidated_table]['reference_column']}"
            )
            if table_columns_filter:
                if 'where' not in query:
                    query = f"{query} where"
                for i, column in enumerate(table_columns_filter):
                    if i > 0:
                        query = f"{query} or"  # TODO: AND ou OR ???
                    query = (f"{query} ({consolidated_table}.\"{column}\" is not null"
                             f" and TRIM(CAST({consolidated_table}.\"{column}\" AS TEXT)) != '')")
            cursor.execute(query)
            columns_description = [i[0] for i in cursor.description]
            for i, column in enumerate(columns_description):
                if table_columns_filter is not None:
                    if column in consolidated_table_columns_filter_names:
                        columns_description[
                            i] = f'{consolidated_table}_{consolidated_table_columns_filter_names[column]}'
            ids = {}  # id => cursor-row-number
            for i, data in enumerate(self.__unpack_results(cursor)):
                _id, *other_fields = data
                if _id not in ids:
                    ids[_id] = []
                ids[_id].append(i)

            for row in self.get_table_related_data(table=reference, table_internal_id=list(ids.keys()),
                                                   reference_table=referenced_table):
                for row_id in ids[row[f'{reference}_internal_id']]:
                    cursor.scroll(row_id, 'absolute')
                    _id, *other_fields = cursor.fetchone()
                    other_fields = dict(zip(columns_description[1:], other_fields))
                    row |= other_fields
                yield row

    def get_pair_fields(self):
        with self.__connection_2.cursor() as cursor_2:
            cursor_2.execute(f"""select table_name, field_a, field_b from {self.__schema}.table_pair_field;""")
            columns = []
            for table_name, field_a, field_b, in self.__unpack_results(cursor_2):
                columns.append(dict(table=table_name, column=field_a))
                columns.append(dict(table=table_name, column=field_b))  # TODO: separação é importante??
            return columns

    def get_sub_pair_fields(self):
        with self.__connection_2.cursor() as cursor_2:
            cursor_2.execute(f"""
            select 
                pair.table_name,
                pair.field_a,
                pair.field_b,
                sub_pair.internal_id,
                sub_pair.field_sub_a,
                sub_pair.field_sub_b
            from {self.__schema}.table_sub_pair_field sub_pair
            join {self.__schema}.table_pair_field pair
                on pair.internal_id = sub_pair.table_pair_field_id;
            """)
            columns = []
            for table_name, field_a, field_b, internal_id, field_sub_a, field_sub_b in self.__unpack_results(cursor_2):
                columns.append(
                    dict(
                        table=table_name,
                        field_a=field_a,
                        field_b=field_b,
                        internal_id=internal_id,
                        field_sub_a=field_sub_a,
                        field_sub_b=field_sub_b,
                    )
                )
            return columns

    def get_counts_data(self, fields: list, cache: bool = False):
        """
        Retorna IDs e Accesions organizados por categorias de acordo com o(s) campo(s) informados. Ideal para campos curtos.

        :param cache: Usar cache em todas as buscas.
        :param fields: lista contendo campos a serem avaliados. Se houverem campos que são pares, usar um dicionario,
        do tipo {campo_chave: campo_valor}, {campo_chave_2: campo_valor_2}.
        :return:
        """
        if cache:
            if self.mining is None:
                self.mining = Mining()
        mining = self.mining if cache else Mining()
        tables = self.get_tables_columns()
        results = {}
        combined_names = []
        with StatusBar('Searching for synomnyms', len(fields)) as status_bar:
            for field_name in fields:
                other_fields = []
                if isinstance(field_name, dict):
                    temp = list(field_name.keys())[0]
                    other_fields = field_name[temp]
                    if isinstance(other_fields, str):
                        other_fields = (other_fields,)
                    field_name = temp
                    del temp
                for i, table in enumerate(tables):
                    if field_name in tables[table]['columns']:
                        query = self.get_query(table)
                        # lista de termos localizados para agrupamento
                        terms = set()
                        other_terms = {other_field: {} for other_field in other_fields}
                        # armazena ids de cada termo para combinação posterior
                        temp_ids = {}
                        with self.__connection.cursor() as cursor:
                            cursor.execute(query['query'])
                            if self.__limit_record_number:
                                cursor.execute(
                                    query['query'] + f' ORDER BY RANDOM() limit {self.__limit_record_number}')
                            columns = ([column.name for column in cursor.description])
                            index = columns.index(field_name)
                            index_accesion = columns.index(query['accession_field'])
                            index_id = columns.index(query['id_field'])
                            other_index_ids = {other_field: columns.index(other_field) for other_field in other_fields}
                            for row in self.__unpack_results(cursor):
                                # ignorando linhas nulas
                                if row[index] is None:
                                    continue
                                term = row[index].lower()
                                terms.add(term)
                                if term not in temp_ids:
                                    temp_ids[term] = {'id': set(), 'accession': set(), 'others': {}}
                                temp_ids[term]['accession'].add(row[index_accesion])
                                temp_ids[term]['id'].add(row[index_id])
                                for other in other_index_ids:
                                    other_term = row[other_index_ids[other]]
                                    if other_term is not None:
                                        other_term = other_term.lower()
                                        if other not in temp_ids[term]['others']:
                                            temp_ids[term]['others'][other] = {}
                                        if other_term not in temp_ids[term]['others'][other]:
                                            temp_ids[term]['others'][other][other_term] = {'id': set(),
                                                                                           'accession': set()}
                                        temp_ids[term]['others'][other][other_term]['accession'].add(
                                            row[index_accesion])
                                        temp_ids[term]['others'][other][other_term]['id'].add(row[index_id])
                        # procurando por sinônimos
                        terms = mining.get_synonyms(terms, terms, cache=cache)
                        temp_results = {}
                        for key in terms:
                            combined_key_name = []
                            for temp in sorted(terms[key]):
                                combined_key_name.append(temp)
                                combined_names.append({'table': table, 'field': temp})  # TODO: testar
                            combined_key_name = '/'.join(combined_key_name)  # TODO: acertar nome. (remover barra)
                            for term in terms[key]:
                                if combined_key_name not in temp_results:  # compound factor: experimental
                                    if term in temp_ids:
                                        temp_results[combined_key_name] = dict(temp_ids[term])
                                    else:
                                        pass  # TODO: ver se precisa criar quando não houver
                                else:
                                    if term in temp_ids:
                                        if 'id' in temp_ids[term]:
                                            temp_results[combined_key_name]['id'] = temp_results[combined_key_name][
                                                'id'].union(
                                                temp_ids[term]['id'])
                                        if 'accession' in temp_ids[term]:
                                            temp_results[combined_key_name]['accession'] = \
                                                temp_results[combined_key_name][
                                                    'accession'].union(
                                                    temp_ids[term]['accession'])
                                        if 'others' in temp_ids[term]:
                                            for other in temp_ids[term]['others']:
                                                if other not in temp_results[combined_key_name]['others']:
                                                    temp_results[combined_key_name]['others'][other] = dict(
                                                        temp_ids[term]['others'][other])
                                                else:
                                                    for other_term in temp_ids[term]['others'][other]:
                                                        if other_term not in (
                                                                temp_results[combined_key_name]['others'][other]):
                                                            temp_results[combined_key_name]['others'][other][
                                                                other_term] = dict(
                                                                temp_ids[term]['others'][other][other_term])
                                                        else:
                                                            temp_results[combined_key_name]['others'][other][
                                                                other_term]['id'] = \
                                                                temp_results[
                                                                    combined_key_name]['others'][other][other_term][
                                                                    'id'].union(
                                                                    temp_ids[term]['others'][other][other_term]['id'])
                                                            temp_results[combined_key_name]['others'][other][
                                                                other_term][
                                                                'accession'] = \
                                                                temp_results[
                                                                    combined_key_name]['others'][other][other_term][
                                                                    'accession'].union(
                                                                    temp_ids[term]['others'][other][other_term][
                                                                        'accession'])
                        if tables[table]['name'] not in results:
                            results[tables[table]['name']] = {field_name: temp_results}
                        else:
                            results[tables[table]['name']][field_name] = temp_results
                status_bar.update()

        def get_count_data_step_1(_results, _table, _field, _category, _mining, _cache):
            for sub_field in _results[_table][_field][_category]['others']:
                _terms = [sub_category for sub_category in _results[_table][_field][_category]['others'][sub_field]]
                for _term in _terms:
                    if _term.startswith(_category) or _term.endswith(_category):
                        new_term = _term.replace(_category, '').strip()
                        with Lock():
                            if new_term not in _terms:
                                _terms.append(new_term)
                                _results[_table][_field][_category]['others'][sub_field][new_term] = \
                                    _results[_table][_field][_category]['others'][sub_field][_term]
                            else:
                                _results[_table][_field][_category]['others'][sub_field][new_term]['id'] = \
                                    _results[_table][_field][_category]['others'][sub_field][new_term]['id'].union(
                                        _results[_table][_field][_category]['others'][sub_field][_term]['id'])
                                _results[_table][_field][_category]['others'][sub_field][new_term]['accession'] = \
                                    _results[_table][_field][_category]['others'][sub_field][new_term][
                                        'accession'].union(
                                        _results[_table][_field][_category]['others'][sub_field][_term]['accession'])
                            _terms.remove(_term)
                            del _results[_table][_field][_category]['others'][sub_field][_term]
                _terms = _mining.get_synonyms(_terms, _terms, cache=_cache)
                for _key in _terms:
                    for _term in _terms[_key]:
                        if _term != _key:
                            with Lock():
                                if (
                                        key in _results[_table][_field][_category]['others'][sub_field]
                                        and _key in _results[_table][_field][_category]['others'][sub_field]
                                        and term in _results[_table][_field][_category]['others'][sub_field]
                                        and _term in _results[_table][_field][_category]['others'][sub_field]
                                ):
                                    _results[_table][_field][_category]['others'][sub_field][_key]['id'] = \
                                        _results[_table][_field][_category]['others'][sub_field][_key]['id'].union(
                                            _results[_table][_field][_category]['others'][sub_field][_term]['id'])
                                    _results[_table][_field][_category]['others'][sub_field][_key]['accession'] = \
                                        _results[_table][_field][_category]['others'][sub_field][_key][
                                            'accession'].union(
                                            _results[_table][_field][_category]['others'][sub_field][_term][
                                                'accession'])
                                    del _results[_table][_field][_category]['others'][sub_field][_term]

        def get_count_data_step_2(_results, _table, _field, _mining):
            _categories = set(_results[_table][_field].keys())
            _to_combine = {}
            for _category in sorted(_categories):
                _synonyms = _mining.get_synonyms(subject=_category, query=_categories, cache=False,
                                                 remove_inverted_repeated=True)
                if _synonyms:
                    for _synonym in _synonyms[_category]:
                        if _category != _synonym:
                            if _category not in _to_combine:
                                _to_combine[_category] = set()
                            _to_combine[_category].add(_synonym)
            while _to_combine:
                _key = list(_to_combine.keys())[0]
                _synonyms = _to_combine[_key]
                _to_keep = _results[_table][_field][_key]
                _remove_keys = set()
                for _synonym in _synonyms:
                    with Lock():
                        _remove_keys.add(_synonym)
                        _to_remove = _results[_table][_field][_synonym]
                        _to_keep['id'] = _to_keep['id'].union(_to_remove['id'])
                        _to_keep['accession'] = _to_keep['accession'].union(_to_remove['accession'])
                        for _sub_key in _to_keep['others']:
                            for _item, _value in _to_remove['others'][_sub_key].items():
                                if _item not in _to_keep['others'][_sub_key]:
                                    _to_keep['others'][_sub_key][_item] = _value
                                else:
                                    _to_keep['others'][_sub_key][_item]['id'] = _to_keep['others'][_sub_key][_item][
                                        'id'].union(
                                        _value['id'])
                                    _to_keep['others'][_sub_key][_item]['accession'] = \
                                        _to_keep['others'][_sub_key][_item][
                                            'accession'].union(_value['accession'])
                with Lock():
                    for _synonym in _synonyms:
                        del _to_combine[_synonym]
                        del _results[_table][_field][_synonym]
                    del _to_combine[_key]

        # combinando campos de outras categorias pelos valores
        logging.info('Combining fields with same categories by values.')  # TODO: criar sistema de log
        with ThreadPoolExecutor(max_workers=cpu_count() * 100) as executor:  # TODO: tratar excessões
            for i, table in enumerate(results):
                for field in results[table]:
                    futures = []
                    for category in results[table][field]:
                        futures.append(
                            executor.submit(get_count_data_step_1, results, table, field, category, mining, cache))
                    with StatusBar(f'Combining categories {i + 1} of {len(results)}',
                                   len(futures)) as status_bar:
                        for future in as_completed(futures):
                            if future.exception():
                                raise future.exception()
                            status_bar.update()

        # combinando campos de outras categorias pelas colunas
        logging.info('Combining fields with same categories by columns.')  # TODO: criar sistema de log
        with ThreadPoolExecutor(max_workers=cpu_count() * 100) as executor:  # TODO: tratar excessões
            for i, table in enumerate(results):
                futures = []
                for field in results[table]:
                    futures.append(
                        executor.submit(get_count_data_step_2, results, table, field, mining))
                with StatusBar(f'Combining categories {i + 1} of {len(results)}',
                               len(futures)) as status_bar:
                    for future in as_completed(futures):
                        if future.exception():
                            raise future.exception()
                        status_bar.update()

        # formatando nomes de colunas
        for table in results:
            for field in results[table]:
                rename_columns = set()
                for category in results[table][field]:
                    formatted_name = format_name(category)
                    if category != formatted_name:
                        rename_columns.add((category, formatted_name))
                for category, formatted_name in rename_columns:
                    results[table][field][formatted_name] = results[table][field].pop(category)

        return results

    def get_counts_file(self, output_dir: str, fields: list = None, cache: bool = False, results: dict = None):
        """
        Gera planilhas para visualização dos resultados.

        :param fields: lista contendo campos a serem avaliados. Se houverem campos que são pares, usar um dicionario,
        do tipo {campo_chave: campo_valor}, {campo_chave_2: campo_valor_2}.
        :param output_dir:
        :param cache: usar cache em todas as buscas.
        :param results: dicionário gerado pelo método get_counts_data.
        TODO: salvar dataframes no banco de dados
        """
        if results is None:
            results = self.get_counts_data(fields=fields, cache=cache)
        for table in results:
            table_name = table.replace('experiment_package_', '').split('_')[0]
            with pd.ExcelWriter(path.join(output_dir, f'{table_name}.xlsx')) as writer:
                index = []
                values = []
                for field_name in sorted(results[table]):
                    # sumary
                    total = 0
                    for field in results[table][field_name]:
                        total += len(results[table][field_name][field]['accession'])
                    index.append(field_name)
                    values.append(total)
                dataframe = pd.DataFrame({'total': values}, index=index)
                dataframe = dataframe.sort_index()
                dataframe.to_csv(path.join(output_dir, f'{table_name}_summary_fields.csv'))
                try:
                    dataframe.to_excel(writer, sheet_name='summary_fields')
                except Exception as err:
                    logging.error(err)
                    logging.error(f'table: {table_name}, sheet: summary_fields')

                values_without_other_fields = {}
                for field_name in sorted(results[table]):
                    # categories
                    for field in sorted(results[table][field_name]):
                        # não possui a categoria outros
                        if len(results[table][field_name][field]['others']) == 0:  # single fields
                            if field_name not in values_without_other_fields:
                                values_without_other_fields[field_name] = {}
                            values_without_other_fields[field_name][field] = len(
                                results[table][field_name][field]['accession'])
                        else:  # combined fields
                            index = []
                            values = []
                            for other in results[table][field_name][field]['others']:
                                for category in results[table][field_name][field]['others'][other]:
                                    index.append(category)
                                    values.append(
                                        len(results[table][field_name][field]['others'][other][category]['accession']))
                                dataframe = pd.DataFrame({field: values}, index=index)
                                dataframe = dataframe.sort_index()
                                total = pd.DataFrame({field: [len(results[table][field_name][field]['accession'])]},
                                                     index=['total'])
                                dataframe = pd.concat([dataframe, total])
                                dataframe.to_csv(path.join(output_dir, f'{table_name}_{format_name(field)[:30]}.csv'))
                                try:
                                    dataframe.to_excel(writer, sheet_name=format_name(field)[:30])
                                except Exception as err:
                                    logging.error(err)
                                    logging.error(f'table: {table_name}, sheet: {format_name(field)[:30]}')
                # single fields
                for field_name in sorted(values_without_other_fields):
                    index = []
                    values = []
                    total = 0
                    for field in sorted(values_without_other_fields[field_name]):
                        quantidade = values_without_other_fields[field_name][field]
                        total += quantidade
                        index.append(field)
                        values.append(quantidade)
                    dataframe = pd.DataFrame({field_name: values}, index=index)
                    dataframe = dataframe.sort_index()
                    total = pd.DataFrame({field_name: [total]}, index=['total'])
                    # dataframe = dataframe.append(total)  # TODO: testar
                    dataframe = pd.concat([dataframe, total])
                    dataframe.to_csv(path.join(output_dir, f'{table_name}_{format_name(field_name)[:30]}.csv'))
                    try:
                        dataframe.to_excel(writer, sheet_name=format_name(field_name)[:30])
                    except Exception as err:
                        logging.error(err)
                        logging.error(f'table: {table_name}, sheet: {format_name(field_name)[:30]}')
            dataframe_consolidado = None
            for field_name in sorted(results[table]):
                for field in sorted(results[table][field_name]):
                    for other in sorted(results[table][field_name][field]['others']):
                        index = []
                        values = []
                        for category in sorted(results[table][field_name][field]['others'][other]):
                            for accession in results[table][field_name][field]['others'][other][category]['accession']:
                                index.append(accession)
                                values.append(category)
                        dataframe = pd.DataFrame({field: values}, index=index)
                        if dataframe_consolidado is None:
                            dataframe_consolidado = dataframe
                        else:
                            dataframe_consolidado = dataframe_consolidado.merge(
                                dataframe, how='outer', left_index=True, right_index=True)
            if dataframe_consolidado is not None:
                dataframe_consolidado = dataframe_consolidado.sort_index()
                dataframe_consolidado.to_csv(path.join(output_dir, f'consolidated_{table_name}.csv'))
                engine = create_engine(f"postgresql://{self.__parameters['user']}"
                                       f":{self.__parameters['password']}"
                                       f"@{self.__parameters['host']}"
                                       f":{self.__parameters['port']}"
                                       f"/{self.__parameters['database']}")
                with engine.begin() as connection:
                    dataframe_consolidado.to_sql(name=f'consolidated_{table_name}', con=connection,
                                                 schema=self.__schema, if_exists='replace', index=True,
                                                 index_label='accession', chunksize=self.__chunk_size)
            try:
                dataframe_consolidado.to_excel(path.join(output_dir, f'consolidated_{table_name}.xlsx'))
            except Exception as err:
                logging.error(err)
                logging.error(f'table: {table_name}, sheet: consolidated')

    def get_query(self, table):
        tables = self.get_tables_columns()
        references = self.get_tables()
        tree = []
        reference = table
        while reference is not None:
            reference = references[reference]['reference'] if reference in references else None
            if reference is not None:
                tree.append(reference)
        query = f"""select {table}.*"""
        join = ''
        accession_field = None
        id_field = None
        for i, reference in enumerate(tree):
            actual = tree[i - 1] if i > 0 else table
            query += f""", {reference}.internal_id as {reference}_internal_id"""
            if reference in tables and 'accession' in tables[reference]['columns']:
                accession_field = f'{reference}_accession'
                id_field = f'{reference}_internal_id'
                query += f""", {reference}.accession as {reference}_accession"""
            for field in tables[actual]['columns']:
                if field == f'{reference}_id':
                    join += f""" join {reference} on {actual}.{field} = {reference}.internal_id"""
        query = f"""{query} from {table} {join}"""
        return {'accession_field': accession_field, 'id_field': id_field, 'query': query}

    # resume long names
    @staticmethod
    def get_short_name(long_name):
        max_word_size = 2
        max_term_size = 60
        temp_name = long_name
        if len(long_name) > max_term_size:
            temp = []
            for name in long_name.split('_')[:-1]:
                temp.append(name[:max_word_size])
            temp.append(long_name.split('_')[-1])
            temp_name = '_'.join(temp)
        return temp_name

    def get_tables(self):
        with self.__connection.cursor() as cursor:
            query = f"""select
                id,
                name,
                reference
            from
                table_name
            order by name"""
            cursor.execute(query)
            tables = {}
            for _id, name, reference in self.__unpack_results(cursor):
                tables[_id] = {'name': name, 'reference': reference}
            for _id in tables:
                if tables[_id]['reference'] in tables:
                    tables[_id]['reference_name'] = tables[tables[_id]['reference']]['name']
                else:
                    tables[_id]['reference_name'] = tables[_id]['reference']
            return tables

    def get_table_data(self, schema, table, columns: list = None, column_key_field: str = None):
        with self.__connection.cursor() as cursor:
            query = f"""select {'*' if columns is None else ', '.join([f'"{column}"' for column in columns])} from {schema}.{table}"""
            if self.__limit_record_number is not None:
                query = f"""{query} order by random() limit {self.__limit_record_number}"""
            cursor.execute(query)
            columns = [i.name for i in cursor.description]
            for row in self.__unpack_results(cursor):
                row = dict(zip(columns, row))
                for column in columns:
                    value = row[column]
                    if value is None:
                        del row[column]
                    elif len(str(value).strip()) == 0:
                        del row[column]
                if column_key_field is not None:
                    if len(row) == 1 and column_key_field in row:
                        del row[column_key_field]
                if row:
                    yield row

    def get_tables_columns(self, table_name: str = None, show_type: bool = False, data_mining_tables=False):
        with self.__connection.cursor() as cursor:
            parameters = [self.__database]
            query = f"""select columns.table_schema, columns.table_name as id, table_name.name,
                "columns".column_name, "columns".data_type
            from information_schema."columns"
            join public.table_name on public.table_name.id = "columns".table_name
            where columns.table_catalog = %s
                and columns.table_schema = 'public'"""
            if table_name is not None:
                query += """ and columns.table_name = %s"""
                parameters.append(table_name)
            if data_mining_tables:
                query += f"""
                union
                select columns.table_schema, "columns".table_name as id, "columns".table_name as name, 
                    "columns".column_name, "columns".data_type
                from information_schema."columns"
                where columns.table_catalog = %s
                    and columns.table_schema = '{self.__schema}'
                """
                parameters.append(self.__database)
            query += """ order by name, column_name;"""
            cursor.execute(query, parameters)
            tables = {}
            for schema_name, _id, name, column_name, data_type in self.__unpack_results(cursor):
                if schema_name == self.__schema:
                    if not _id.startswith('consolidated_'):
                        continue
                if show_type:
                    column = {'name': column_name, 'data_type': data_type, 'structured': True}
                else:
                    column = column_name
                if _id not in tables:
                    tables[_id] = {'schema': schema_name, 'name': name, 'columns': [column]}
                else:
                    tables[_id]['columns'].append(column)
            if show_type:
                # classifying data with scructured or not.
                for table in sorted(tables):
                    columns = []
                    for column in tables[table]['columns']:
                        if column['data_type'] == 'text':
                            columns.append(column['name'])
                    for column in columns:
                        query = f"select max(cardinality(string_to_array(\"{column}\", ' ')))" \
                                f"from {tables[table]['schema']}.{table} " \
                                f"where \"{column}\" is not null;"
                        cursor.execute(query)
                        word_size, = cursor.fetchone()
                        for field in tables[table]['columns']:
                            if field['name'] == column:
                                i = tables[table]['columns'].index(field)
                                if word_size is not None:
                                    if word_size > self.__word_size_length_unstructured:
                                        tables[table]['columns'][i]['structured'] = False
                                break
            if table_name is not None:
                return tables[table_name]
            else:
                return tables

    def get_table_count(self, table_name):
        with self.__connection.cursor() as cursor:
            query = f"""SELECT id, name from table_name;"""
            cursor.execute(query)

            table, name = cursor.fetchall()
            query = f"""SELECT COUNT(*) AS records_number FROM {table};"""
            cursor.execute(query, (table,))
            records_number, = cursor.fetchone()
            return table, name, records_number

    def get_tables_count(self):
        with self.__connection.cursor() as cursor:
            query = f"""SELECT id, name from table_name;"""
            cursor.execute(query)

            for table, name in cursor.fetchall():
                query = f"""SELECT COUNT(*) AS records_number FROM {table};"""
                cursor.execute(query, (table,))
                records_number, = cursor.fetchone()
                yield table, name, records_number

    def get_table_column_count(self, schema: str, table_name: str, columns: list):
        query = "select "
        for i, column in enumerate(columns):
            if i > 0:
                query += ", "
            query += f"count(\"{column}\") as \"{column}\""
        query += f" from {schema}.{table_name};"
        with self.__connection.cursor() as cursor:
            cursor.execute(query)
            return dict(zip([i[0] for i in cursor.description], cursor.fetchone()))

    def get_table_combined_column_count(self, schema: str, table_name: str, columns: list):
        query = "select count(*) as total"
        query += f" from {schema}.{table_name}"
        for i, column in enumerate(columns):
            if i == 0:
                query += " where "
            else:
                query += " or "
            query += f'("{column}" is not null and trim(cast("{column}" as text)) != \'\')'
        with self.__connection.cursor() as cursor:
            cursor.execute(query)
            return dict(zip([i[0] for i in cursor.description], cursor.fetchone()))

    def get_table_references(self, table: str, target: str = None):
        """
        Show references from table to target table.
        :param table: Table name (on database schema).
        :param target: Target table. If None target will be the root table.
        :return: List of references tables.
        """
        path = [{'name': table}]
        with self.__connection.cursor() as cursor:
            query = f"""select id, name, reference from table_name where id = %s"""
            while True:
                cursor.execute(query, (path[-1]['name'],))
                record = cursor.fetchone()
                if not record:
                    break
                _id, name, reference = record
                path[-1]['destination table'] = reference
                path[-1]['destination key'] = 'internal_id'
                path[-1]['origin key'] = f'{reference}_id'
                path.append({'name': reference})
                if reference == target:
                    break
            if path[-1]['name'] != target:
                cursor.execute(query, (target,))
                record = cursor.fetchone()
                if record:
                    _id, name, reference = record
                    path[-1]['destination table'] = target
                    path[-1]['destination key'] = f'{reference}_id'
                    path[-1]['origin key'] = 'internal_id'
        return path

    def get_table_related_data(self, table, reference_table, table_internal_id=None,
                               table_columns_filter: list | set = None):
        with self.__connection.cursor() as cursor:
            # recuperando referências até "reference_table"
            references = self.get_table_references(table, reference_table)
            table_columns_description = self.get_tables_columns(table_name=table, show_type=True)['columns']
            table_columns = [column['name'] for column in table_columns_description]
            reference_columns = self.get_tables_columns(reference_table)['columns']
            # montando  consulta
            query = (
                f"select {', '.join([f'{table}.{column} as {table}_{column}' for column in table_columns])}"
                f", {', '.join([f'{reference_table}.{column} as {reference_table}_{column}' for column in reference_columns])}"
            )
            for i, table_name in enumerate(references):
                if i == 0:
                    query = f"{query} from {table_name['name']}"
                if table == reference_table:
                    continue
                # TODO: checar necessidade dessa instrução
                # if 'destination table' in table_name and f"from {reference_table}" in query and table_name['destination table'] == reference_table:
                #     continue
                if 'destination table' in table_name:
                    query = (
                        f"{query} join {table_name['destination table']} on {table_name['name']}.{table_name['origin key']}"
                        f" = {table_name['destination table']}.{table_name['destination key']}"
                    )
                # TODO: checar necessidade dessa instrução
                # else:
                #     query = (
                #         f"{query} join {table_name['name']} on {table}.{table_name['name']}_id"
                #         f" = {table_name['name']}.internal_id"
                #     )
            # filtrando por id do registro
            if table_internal_id:
                if 'where' not in query:
                    query = f"{query} where"
                if isinstance(table_internal_id, list) or isinstance(table_internal_id, tuple):
                    # query = f"{query} {table}.internal_id in (%s)"
                    query = f"{query} {table}.internal_id in "
                else:
                    query = f"{query} {table}.internal_id = %s"
            # filtrando por presença de valores em colunas
            if table_columns_filter:
                if 'where' not in query:
                    query = f"{query} where"
                for i, column in enumerate(table_columns_filter):
                    if i > 0:
                        query = f"{query} or"
                    query = f"{query} {table}.{column} is not null"
                    """and {table}.{column} != ''"""  # TODO: inserir
            if table_internal_id:
                if isinstance(table_internal_id, list) or isinstance(table_internal_id, tuple):
                    query = f"{query} ({', '.join([str(i) for i in table_internal_id])})"
            # aplicando limites de registros
            if self.__limit_record_number:
                query += f" order by random() limit {self.__limit_record_number}"
            if table_internal_id:
                cursor.execute(query, (table_internal_id,))
            else:
                cursor.execute(query)
            column_names = [f'{table}_{column}' for column in table_columns]
            column_names.extend([f'{reference_table}_{column}' for column in reference_columns])
            for row in self.__unpack_results(cursor):
                yield dict(zip(column_names, row))

    # carrega registros de uma tabela
    def get_values_from_table(self, table: str, ids: list | set = None, schema: str = 'public'):
        query = f"select * from {schema}.{table}"
        if ids:
            query += f" where internal_id in ({', '.join([str(i) for i in ids])})"
        query += ";"
        with self.__connection.cursor() as cursor:
            cursor.execute(query)
            columns_names = [i[0] for i in cursor.description]
            for row in self.__unpack_results(cursor):
                yield dict(zip(columns_names, row))

    # carrega registros de uma tabela
    def get_values_from_table_by_value(self, table: str, column: str, value: str | list | set, schema: str = 'public'):
        query = f"select * from {schema}.{table}"
        if isinstance(value, str):
            query = f"{query} where {column} ilike '{value}'"
        elif isinstance(value, list) or isinstance(value, set):
            values = ','.join([f"'{i}'" for i in value])
            query = f"{query} where lower({column}) in ({values})"
        with self.__connection.cursor() as cursor:
            cursor.execute(query)
            columns_names = [i[0] for i in cursor.description]
            for row in self.__unpack_results(cursor):
                yield dict(zip(columns_names, row))

    def insert_ignored_table_join_fields(self, schema_a, table_a, field_a, schema_b, table_b, field_b):
        tables = self.get_tables_columns(data_mining_tables=True)
        for table in [table_a, table_b]:
            if table not in tables:
                raise KeyError(f'Table {table} does not exist.')
        for table in tables:
            if tables[table]['schema'] == schema_a and table == table_a:
                if field_a not in tables[table]['columns']:
                    raise KeyError(f'Invalid field "{field_a}" for table {table_a} in schema {schema_a}.')
            elif tables[table]['schema'] == schema_b and table == table_b:
                if field_b not in tables[table]['columns']:
                    raise KeyError(f'Invalid field "{field_b}" for table {table_b} in schema {schema_b}.')
        with self.__connection.cursor() as cursor:
            query = f"""
            select count(*)
            from {self.__schema}.ignored_table_join_fields
            where schema_a = %s and table_a = %s and field_a = %s and schema_b = %s and table_b = %s and field_b = %s
            """
            cursor.execute(query, (schema_a, table_a, field_a, schema_b, table_b, field_b))
            number, = cursor.fetchone()
            if number == 0:
                cursor.execute(query, (schema_b, table_b, field_b, schema_a, table_a, field_a))
                number, = cursor.fetchone()
            if number == 0:
                query = f"""
                insert into {self.__schema}.ignored_table_join_fields (schema_a, table_a, field_a, schema_b, table_b, field_b)
                values (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query, (schema_a, table_a, field_a, schema_b, table_b, field_b))
                self.__connection.commit()
                return True
            else:
                return False

    def insert_table_join_fields(self, schema_a, table_a, field_a, schema_b, table_b, field_b):
        tables = self.get_tables_columns(data_mining_tables=True)
        for table in [table_a, table_b]:
            if table not in tables:
                raise KeyError(f'Table {table} does not exist.')
        for table in tables:
            if tables[table]['schema'] == schema_a and table == table_a:
                if field_a not in tables[table]['columns']:
                    raise KeyError(f'Invalid field "{field_a}" for table {table_a} in schema {schema_a}.')
            elif tables[table]['schema'] == schema_b and table == table_b:
                if field_b not in tables[table]['columns']:
                    raise KeyError(f'Invalid field "{field_b}" for table {table_b} in schema {schema_b}.')
        with self.__connection.cursor() as cursor:
            query = f"""
            select count(*)
            from {self.__schema}.table_join_fields
            where schema_a = %s and table_a = %s and field_a = %s and schema_b = %s and table_b = %s and field_b = %s
            """
            cursor.execute(query, (schema_a, table_a, field_a, schema_b, table_b, field_b))
            number, = cursor.fetchone()
            if number == 0:
                cursor.execute(query, (schema_b, table_b, field_b, schema_a, table_a, field_a))
                number, = cursor.fetchone()
            if number == 0:
                query = f"""
                insert into {self.__schema}.table_join_fields (schema_a, table_a, field_a, schema_b, table_b, field_b)
                values (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query, (schema_a, table_a, field_a, schema_b, table_b, field_b))
                self.__connection.commit()
                return True
            else:
                return False

    # register table pair fields
    def insert_table_pair_fields(self, table, field_a, field_b):
        with self.__connection.cursor() as cursor:
            cursor.execute(f"""
            select count(*)
            from {self.__schema}.table_pair_field
            where table_name = %s and field_a  = %s and field_b = %s;
            """, (table, field_a, field_b))
            number, = cursor.fetchone()
            if number == 0:
                cursor.execute(f"""
                insert into {self.__schema}.table_pair_field(table_name, field_a, field_b)
                 values(%s, %s, %s);
                """, (table, field_a, field_b))
                self.__connection.commit()

                cursor.execute(f"""
                select internal_id 
                from {self.__schema}.table_pair_field 
                where table_name = %s and field_a = %s and field_b = %s
                """, (table, field_a, field_b))
                internal_id, = cursor.fetchone()

                cursor.execute(f"""
                select distinct({field_a}) from {table}
                """)
                temp_file_sub_fields = NamedTemporaryFile()
                sep = '<===>'
                with open(temp_file_sub_fields.name, 'w') as file:
                    for field, in self.__unpack_results(cursor):
                        field = [i.strip() for i in field.split(':')]
                        if len(field) == 2:
                            file.write(f'{sep.join(field)}\n')
                with open(temp_file_sub_fields.name, 'r') as file:
                    for line in file:
                        field_sub_a, field_sub_b = [i.strip() for i in line.split(sep)]  # TODO: limpeza
                        if len(field_sub_b) > 1:
                            cursor.execute(f"""
                            insert into {self.__schema}.table_sub_pair_field(field_sub_a, field_sub_b, table_pair_field_id)
                            values (%s, %s, %s)
                            """, (field_sub_a, field_sub_b, internal_id))

    def insert_table_unstructured_fields(self, schema, table, fields):
        with self.__connection.cursor() as cursor:
            for field in fields:
                cursor.execute(f"""
                select count(*)
                from {self.__schema}.table_unstructured_field
                where schema_name = %s and table_name = %s and field = %s;
                """, (schema, table, field))
                number, = cursor.fetchone()
                if number == 0:
                    cursor.execute(f"""insert into {self.__schema}.table_unstructured_field(schema_name, table_name, field)
                    values (%s, %s, %s);""", (schema, table, field))
            self.__connection.commit()

    # insert database data
    def insert_from_xml(self, element: etree.ElementTree, entities: dict, references: dict, paths: dict,
                        last_ids: dict = None, internal_id: int = 1, foreign_key: dict = None, start_node=None):
        try:
            path = element.getroottree().getelementpath(element)
            if last_ids is None:
                last_ids = {}
            if foreign_key is None:
                foreign_key = {}
            if element.getroottree().getelementpath(element) == '.':
                entity = format_name(element.tag)
            else:
                entity = format_name(path)
            foreign_key[entity] = internal_id
            if entity in entities:
                fields = [field for field in entities[entity] if field != 'internal_id']
                fields.append('internal_id')
                if entity in references:
                    fields.append(f'{self.get_short_name(references[entity])}_id')
                query = f'INSERT INTO "{self.get_short_name(entity)}"('
                query += ','.join(fields)
                query += f') VALUES ({",".join(["%s"] * len(fields))});'
                values = {}
                for field in fields:
                    values[field] = None
                for field in entities[entity]:
                    if entities[entity][field]['origin'] == 'attrib':
                        for attrib in element.attrib:
                            if field.lower() == attrib.lower():
                                values[field] = element.get(attrib)
                    elif entities[entity][field]['origin'] == 'text':
                        if element.text is not None:
                            values[field] = element.text
                    elif entities[entity][field]['origin'] == 'tag':
                        child_element = element.find(entities[entity][field]['tag'])
                        values[field] = element.find(entities[entity][field]['tag']).text \
                            if child_element is not None else None
                    elif entities[entity][field]['origin'] == 'tag_empty':
                        values[field] = True if element.find(entities[entity][field]['tag']) is not None else None
                values['internal_id'] = internal_id
                if entity in references:
                    values[f'{self.get_short_name(references[entity])}_id'] = foreign_key[references[entity]]
                with self.__connection.cursor() as cursor:
                    with Lock():
                        cursor.execute(
                            query, [values[field] for field in fields]
                        )
                        self.__connection.commit()
            with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
                for i, element_child in enumerate(element):
                    child_name = format_name(element.getroottree().getelementpath(element_child))
                    if child_name not in last_ids:
                        last_ids[child_name] = 0
                    last_ids[child_name] += 1
                    if start_node == element.tag:
                        executor.submit(self.insert_from_xml, **dict(
                            element=element_child,
                            entities=entities,
                            references=references,
                            paths=paths,
                            last_ids=last_ids,
                            internal_id=last_ids[child_name],
                            foreign_key=foreign_key,
                            start_node=start_node
                        ))
                    else:
                        self.insert_from_xml(element=element_child, entities=entities, references=references,
                                             paths=paths,
                                             last_ids=last_ids, internal_id=last_ids[child_name],
                                             foreign_key=foreign_key)
        except Exception as err:
            logging.error(f"{err}. 'Data insertion failed. Parent: {element.getroottree().getelementpath(element)}")

    def search_by_equal_column(self, schema_name, table_name, cache=False) -> list:
        if cache:
            if self.mining is None:
                self.mining = Mining()
        else:
            self.mining = Mining()
        tables = self.get_tables_columns(data_mining_tables=True)
        table = None
        for i in tables:
            if tables[i]['schema'] == schema_name and i == table_name:
                table = tables[i]
                table['id'] = i
                break
        columns = deepcopy(table['columns'])
        for items in self.get_ignored_fields_with_schema()['columns']:
            if items['schema'] == schema_name and items['table'] == table_name:
                if items['column'] in columns:
                    columns.remove(items['column'])
        # removendo IDs da busca
        for column in table['columns']:
            if column.endswith('id') and column in columns:
                columns.remove(column)
        results = []
        equal_columns = []
        rejected_columns = []
        with self.__connection.cursor() as cursor:
            cursor.execute(f"""
            select field_a, field_b 
            from {self.__schema}.ignored_table_join_fields
            where schema_a = %s and schema_b = schema_a and table_a = %s and table_b = table_a 
            """, (schema_name, table_name))
            for field_a, field_b in self.__unpack_results(cursor):
                rejected_columns.append({field_a, field_b})

            cursor.execute(f"""
            select field_a, field_b 
            from {self.__schema}.table_join_fields
            where schema_a = %s and schema_b = schema_a and table_a = %s and table_b = table_a 
            """, (schema_name, table_name))
            for field_a, field_b in self.__unpack_results(cursor):
                equal_columns.append({field_a, field_b})

        # indexando por sinonimos
        teste_columns = [column.replace('_', ' ') for column in columns]
        synonyms = self.mining.get_synonyms(teste_columns, teste_columns, cache=cache)
        for column_a in synonyms:
            for column_b in synonyms[column_a]:
                column_a = column_a.replace(' ', '_')
                column_b = column_b.replace(' ', '_')
                if column_a != column_b:
                    if {column_a, column_b} not in equal_columns and {column_a, column_b} not in rejected_columns:
                        if (column_a, column_b) not in results and (column_b, column_a) not in results:
                            results.append((column_a, column_b))

        for column_a in columns:
            if len(column_a) >= 3:
                for column_b in sorted(set(columns) - {column_a}):
                    if {column_a, column_b} not in equal_columns and {column_a, column_b} not in rejected_columns:
                        if '_' in column_b:
                            if column_a in column_b.split('_'):
                                if (column_a, column_b) not in results and (column_b, column_a) not in results:
                                    results.append((column_a, column_b))
        return results

    @staticmethod
    def __unpack_results(cursor, size=10000):
        while True:
            rows = cursor.fetchmany(size)
            if len(rows) == 0:
                break
            else:
                for row in rows:
                    yield row

    def __del__(self):
        if self.__connection:
            self.__connection.close()
