#!/usr/bin/env python3

from argparse import ArgumentParser
from data.util import NONE_VALUES
from os import makedirs
from os import path
from venn import venn
import csv
import matplotlib.pyplot as plt
import progressbar

if __name__ == '__main__':
    args = ArgumentParser()
    args.add_argument('--input', help='Input terms csv file. Generate by check_community extract_fields option.', required=True)
    args.add_argument('--output', help='Output directory.', required=True)
    args = args.parse_args()

    arquivo = args.input
    diretorio_saida = args.output

    if not path.exists(diretorio_saida):
        makedirs(diretorio_saida)

    dataset = {}
    dataset_conciliated = {}

    print('Loading data...')
    with open(arquivo, 'r') as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        work_columns = set()
        for column in header:
            dataset[column] = list()
            if column not in ['node_a', 'node_b']:
                work_columns.add(column)
                dataset_conciliated[column] = set()
        for row in csv_reader:
            record = dict(zip(header, row))
            for column in header:
                value = record[column].strip().lower()
                dataset[column].append(value if value not in NONE_VALUES else None)
                if column in work_columns:
                    if value not in NONE_VALUES:
                        dataset_conciliated[column].add(record['node_a'])
                        dataset_conciliated[column].add(record['node_b'])

    print('Processing totals...')
    totals = {column: len(dataset_conciliated[column]) for column in dataset_conciliated}
    sorted_totals = [total[0] for total in sorted(totals.items(), reverse=True, key=lambda x: x[1])]

    print('Summarizing...')
    with open(path.join(diretorio_saida, 'summary.csv'), 'w') as summary:
        summary.write('type,base_column,attributes,nodes\n')
        # considerando todas as correspondências preenchidas
        with progressbar.ProgressBar(max_value=len(sorted_totals)) as bar:
            for column in sorted_totals:
                keep_columns = []
                nodes = {node for node in dataset_conciliated[column]}
                test_columns = [c for c in sorted_totals if c != column]
                for column2 in test_columns:
                    if len(dataset_conciliated[column] & dataset_conciliated[column2]) > 0:
                        keep_columns.append(column2)
                        nodes &= dataset_conciliated[column2]
                summary.write(','.join(map(str, ['all correspondencies', column, len(keep_columns)+1, len(nodes)])))
                summary.write('\n')

                # considerando até cinco correspondências preenchidas
                nodes_venn = {node for node in dataset_conciliated[column]}
                header_columns = [column]
                for i, column2 in enumerate(keep_columns, start=2):
                    header_columns.append(column2)
                    nodes_venn &= dataset_conciliated[column2]
                    summary.write(','.join(map(str, [f'{i} correspondencies', column, i, len(nodes_venn)])) + '\n')
                    with open(path.join(diretorio_saida, f'{column}_{i}_correspondencies.csv'), 'w') as f2:
                        header2 = ['accession'] + header_columns
                        f2.write(','.join(header2) + '\n')
                        for j, accessions in enumerate(zip(dataset['node_a'], dataset['node_b'])):
                            node_a, node_b = accessions
                            if node_a in nodes_venn or node_b in nodes_venn:
                                value = []
                                for c in header_columns:
                                    value.append(dataset[c][j] if dataset[c][j] is not None else '')
                                if node_a in nodes_venn:
                                    f2.write(','.join([dataset['node_a'][j]] + value) + '\n')
                                if node_b in nodes_venn:
                                    f2.write(','.join([dataset['node_b'][j]] + value) + '\n')
                    if i in (2, 3, 4, 5, 6):
                        diagram = {c: set() for c in header_columns[:i]}
                        for c in header_columns[:i]:
                            diagram[c] = dataset_conciliated[c]
                        for key in list(diagram.keys()):
                            if len(diagram[key]) == 0:
                                del diagram[key]
                        if len(diagram) > 0:
                            venn(diagram)
                            plt.savefig(path.join(diretorio_saida, f'{column}_{i}_correspondencies.png'), dpi=600)
                            plt.savefig(path.join(diretorio_saida, f'{column}_{i}_correspondencies.svg'))
                            plt.close()

                bar.update(bar.value + 1)
