#!/usr/bin/env python3

import argparse
from os import path, makedirs

import matplotlib.pyplot as plt
import networkx as nx
import csv

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--input', type=str, help='Network file.')
    arg_parser.add_argument('--output', type=str, help='Output directory.')

    args = arg_parser.parse_args()

    G = nx.Graph()

    print('Loading network data...')
    with open(args.input, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            row = dict(zip(header, row))
            weight = float(row['weight'])
            G.add_edge(row['node_a'], row['node_b'], weight=weight, width=weight)

    print('Calculating edge weights...')
    edges = G.edges()
    weights = [G[u][v]['weight'] for u, v in edges]
    widths = [G[u][v]['width'] for u, v in edges]

    print('Plotting network...')
    nx.draw(
        G,
        node_size=1,
        with_labels=False,
        pos=nx.spring_layout(G),
        width=widths,
        edge_color='lightblue',
    )

    print('Saving network files...')
    if not path.exists(args.output):
        makedirs(args.output)
    name = path.split(args.input)[-1].rsplit('.', 1)[0]
    plt.savefig(path.join(args.output, f'{name}_network.png'), dpi=600)
    plt.savefig(path.join(args.output, f'{name}_network.svg'))
