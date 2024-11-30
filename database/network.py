#!/usr/bin/env python3

import json
from argparse import ArgumentParser
from datetime import datetime
from os import makedirs
from os import path
import time

from data.network import Network
from data.util import dict_to_csv

if __name__ == '__main__':
    arguments = ArgumentParser()

    arguments.add_argument(
        '--column_key_name',
        help='Name of the column containing IDs.',
        required=False,
        default='experiment_package_sample_accession')
    arguments.add_argument('--input', help='Network csv input file.', required=True)
    arguments.add_argument(
        '--is_graph_file',
        action='store_true',
        help='Network csv input file is a grapsh result file.',
        required=False,
        default=False)
    arguments.add_argument('--generate_entire_network', action='store_true',
                           help='Add entire network graph to results.', required=False, default=False)
    arguments.add_argument('--prefix', help='File name prefix.', required=False)
    arguments.add_argument('--work_directory', help='Network csv input file.', required=True)
    arguments.add_argument('--debug', help='Debug mode.', action='store_true', default=False)
    arguments.add_argument('--thread', help='Use threading.', action='store_true', default=False)
    arguments.add_argument('--hide_labels', help='Show labels on networks.', action='store_true', default=False)
    arguments.add_argument('--temp_directory', help='Temp directory path. Default system temp directory.')

    arguments = arguments.parse_args()

    if arguments.debug:
        if arguments.thread:
            print('Using threads.')

    start_execution = time.time()

    prefix = arguments.prefix if arguments.prefix is not None else ''
    work_directory = arguments.work_directory
    result_directory = f"network_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"

    print('Loading input data...')

    if not path.exists(work_directory):
        makedirs(work_directory)
    if not path.exists(path.join(work_directory, result_directory)):
        makedirs(path.join(work_directory, result_directory))
    intermediated_files = path.join(work_directory, result_directory, 'intermediated_files')
    if not path.exists(intermediated_files):
        makedirs(intermediated_files)

    network = Network(
        debug=arguments.debug,
        threading=arguments.thread,
        work_directory=intermediated_files,
        temp_directory=arguments.temp_directory
    )
    if arguments.is_graph_file:
        results = network.plot_from_csv_file(csv_file=arguments.input, show_labels=not arguments.hide_labels,
                                             generate_entire_network=arguments.generate_entire_network)
    else:
        network.generate_csv_index(csv_file=arguments.input, column_key_name=arguments.column_key_name)
        results = network.generate_network_from_csv(
            csv_file=arguments.input, column_key_name=arguments.column_key_name, show_labels=not arguments.hide_labels,
            generate_entire_network=arguments.generate_entire_network)

    end_execution = time.time()

    # escrevendo resultados no disco
    with open(path.join(path.dirname(path.realpath(__file__)), 'data', 'template', 'network_index.html')) as file:
        html_index_template = file.read()

    # escrevendo subredes/comunidades
    communities_javascript_objects = []
    for i, result in enumerate(results['communities']):
        nodes_number = str(result['info']['nodes_number'])
        edges_number = str(result['info']['edges_number'])

        html_file_name = f'{prefix}community_{i}.html'
        communities_javascript_objects.append(
            "{id: " + str(i)
            + ", nodes_number: " + nodes_number
            + ", edges_number: " + edges_number
            + ", type_connections: " + str(len(result['info']['labels']))
            + ", description: 'labels:<hr/>" + '<hr/>'.join(
                [str(label).replace("'", "\\'") for label in result['info']['labels']])
            + "', link: '" + html_file_name
            + "'}"
        )
        with open(path.join(work_directory, result_directory, html_file_name), 'w') as file:
            file.write(result['html'])
        with open(path.join(work_directory, result_directory, f'{prefix}community_{i}.json'), 'w') as file:
            json.dump(result['table'], file)
        with open(path.join(work_directory, result_directory, f'{prefix}community_{i}.csv'), 'w') as file:
            dict_to_csv(result['table'], file)

    if 'network' not in results:
        nodes_number = 0
        edges_number = 0
        type_connections_number = 0
        for i, result in enumerate(results['communities']):
            nodes_number += result['info']['nodes_number']
            edges_number += result['info']['edges_number']
            type_connections_number += len(result['info']['labels'])
        html_index_template = html_index_template.replace(
            ':network:',
            "{nodes_number: " + str(nodes_number)
            + ", edges_number: " + str(edges_number)
            + ", type_connections: " + str(type_connections_number)
            + ", description: 'Entire network.', "
            + "link: '#'}"
        )
    else:
        result = results['network']

        nodes_number = str(result['info']['nodes_number'])
        edges_number = str(result['info']['edges_number'])
        type_connections_number = str(len(result['info']['labels']))
        html_file_name = f'{prefix}{"_" if prefix else ""}network.html'
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
                            f'{prefix}{"_" if prefix else ""}network.json'), 'w') as file:
            json.dump(result['table'], file)
        with open(
                path.join(work_directory, result_directory, f'{prefix}{"_" if prefix else ""}network.csv'),
                'w') as file:
            dict_to_csv(result['table'], file)

    html_index_template = html_index_template.replace(':communities:', ','.join(communities_javascript_objects))

    with open(path.join(work_directory, result_directory, f'index.html'), 'w') as file:
        file.write(html_index_template)
