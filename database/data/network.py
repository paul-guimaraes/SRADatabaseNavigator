import csv
from joblib import Parallel, delayed
from math import comb
from os import path
from progressbar import ProgressBar
from tempfile import NamedTemporaryFile
from csv import reader
import pickle

from .pyvisnetwork import PyvisNetwork
from .util import get_combinations, NONE_VALUES


class Network:
    def __init__(self, debug=False, status_callback=None, threading=False, work_directory=None, temp_directory=None):
        self.__debug = debug
        self.__sep_fields = '<==>'
        self.__status_callback = status_callback
        self.__threading = threading
        self.__work_directory = work_directory
        self.__table_index = None
        self.__nodes_index = None
        self.__columns_index = None
        self.__temp_directory = temp_directory
        self.__temp_files = []

    def calculate_edges(self, edges, communities):
        edges_all = edges
        labels_all = []
        weights_all = []

        edges_communities = []
        labels_communities = []
        weights_communities = []

        if self.__debug:
            bar = ProgressBar(maxval=len(edges))
            bar.print('Calculando pesos...')
        self.set_status('Calculating network edges weights.')

        # working on edges weights
        for edge in edges:
            weight = len(edges[edge])
            if self.__nodes_index is not None:
                label = edges[edge]
            else:
                label = ', '.join(edges[edge])
            labels_all.append(label)
            weights_all.append(weight)
            if self.__debug:
                bar.update(bar.value + 1)
        if self.__debug:
            bar.finish()

        # working on communities
        if self.__debug:
            bar = ProgressBar(maxval=len(communities))
            bar.print('Distribuindo dados por comunidade...')
        self.set_status('Distributing data by community.')

        def __get_communities(_community, _edges, _labels_all, _weights_all):
            if isinstance(_community, str):
                _temp_file = _community
                with open(_temp_file, 'rb') as _community:
                    _community = pickle.load(_community)
            _edges_communities = []
            _labels_communities = []
            _weights_communities = []
            for _edge, _label, _weight in zip(_edges, _labels_all, _weights_all):
                a, b = _edge
                if a in _community or b in _community:
                    _edges_communities.append(_edge)
                    _labels_communities.append(_label)
                    _weights_communities.append(_weight)
            if self.__debug:
                bar.update(bar.value + 1)
            return {
                'edges': _edges_communities,
                'labels': _labels_communities,
                'weights': _weights_communities
            }

        if self.__threading:
            results = Parallel(n_jobs=-1)(
                delayed(__get_communities)(community, edges_all, labels_all, weights_all) for community in communities)
        else:
            results = [__get_communities(community, edges_all, labels_all, weights_all) for community in communities]
        if self.__debug:
            bar.finish()

        for result in results:
            edges_communities.append(result['edges'])
            labels_communities.append(result['labels'])
            weights_communities.append(result['weights'])

        result = {
            'edges_all': edges_all,
            'labels_all': labels_all,
            'weights_all': weights_all,

            'edges_communities': edges_communities,
            'labels_communities': labels_communities,
            'weights_communities': weights_communities,
        }
        if self.__work_directory is not None:
            with open(path.join(self.__work_directory, 'calculated_edges.pkl'), 'wb') as file:
                pickle.dump(result, file)
        return result

    def get_communities(self, edges):
        self.set_status('Searching for network communities.')
        communities = []
        if self.__debug:
            bar = ProgressBar(maxval=len(edges))
            bar.print('Processando comunidades...')
        for edge in edges:
            a, b = edge
            for community in communities:
                if a in community or b in community:
                    community.add(a)
                    community.add(b)
                    break
            else:
                community = set()
                community.add(a)
                community.add(b)
                communities.append(community)
            if self.__debug:
                bar.update(bar.value + 1)
        if self.__debug:
            bar.finish()

        join_communities = True
        while join_communities:
            restart = False
            for i, community_a in enumerate(communities):
                if restart:
                    restart = False
                    break
                for community_b in communities:
                    if restart:
                        break
                    if community_a != community_b:
                        if len(community_a.intersection(community_b)) > 0:
                            restart = True
                            communities[i] = community_a.union(community_b)
                            communities.remove(community_b)
            else:
                join_communities = False
        if self.__work_directory is not None:
            with open(path.join(self.__work_directory, 'communities.pkl'), 'wb') as file:
                pickle.dump(communities, file)
        return communities

    def generate_csv_index(self, csv_file: str, column_key_name: str):
        csv_index = {}
        with open(csv_file, 'r') as file:
            csv_reader = csv.reader(file, delimiter=',', quotechar='"')
            columns = next(csv_reader)
            for column in columns:
                csv_index[column] = set()
            for line in csv_reader:
                record = dict(zip(columns, line))
                for column in record:
                    csv_index[column].add(record[column])
        for column in csv_index:
            csv_index[column] = list(csv_index[column])
        self.__table_index = csv_index
        self.__nodes_index = self.__table_index[column_key_name]
        self.__columns_index = columns
        if self.__work_directory is not None:
            with open(path.join(self.__work_directory, 'table_index.pkl'), 'wb') as file:
                pickle.dump(csv_index, file)

    def get_network(self, edges: list, labels: list, weights: list, edges_width_factor: float = 4.5,
                    show_labels=True, suffix: int | str = None) -> dict:
        """
        :param suffix: suffix for result file name.
        :param show_labels: put labels on network.
        :param edges: list of edges in form [(a, b), (c, d), ...].
        :param edges_width_factor: width factor for edges. It's determines the proportion of edges strokes.
        :param labels: list of strings edges labels.
        :param weights: list of floats weights.
        :return: A html temp file with network plot.
        """

        if self.__nodes_index is not None:
            edges = [(self.__nodes_index[edge[0]], self.__nodes_index[edge[1]]) for edge in edges]
            for i, label in enumerate(labels):
                temp = []
                for item in label:
                    attribute = self.__columns_index[item[0]]
                    temp.append(f'{attribute}: {self.__table_index[attribute][item[1]]}')
                labels[i] = ', '.join(sorted(temp))

        # net = network.Network(height='800', width='auto', select_menu=True)
        net = PyvisNetwork(height='800', width='auto', select_menu=True)
        net.set_template(path.join(path.dirname(path.realpath(__file__)), 'template', 'pyvis_template.html'))
        # net.show_buttons(filter_=[
        #     'edges',
        #     'interaction',
        #     'layout',
        #     'manipulation',
        #     'nodes',
        #     'physics',
        #     'renderer',
        #     'selection',
        # ])
        net.set_options("""
            const options = {
                "interaction": {
                    "hideEdgesOnDrag": true,
                    "multiselect": true,
                    "navigationButtons": true,
                    "selectConnectedEdges": false,
                    "hoverConnectedEdges": false,
                    "hover": true,
                    "zoomSpeed": 2
                },
                "physics": {
                    "forceAtlas2Based": {
                    "springLength": 100
                },
                    "minVelocity": 0.75,
                    "solver": "forceAtlas2Based"
                }
            }
        """)

        if len(edges) != len(labels):
            raise ValueError('Number of edges does not match the number of labels')

        if self.__debug:
            print('Processando rede...')
        self.set_status('Processing network.')
        max_weight = max(weights)
        edge_widths = [w / max_weight * edges_width_factor for w in weights]

        nodes = set()

        if self.__debug:
            bar = ProgressBar(maxval=len(edges))
            bar.print('Preparando nós...')
        self.set_status('Processing network nodes.')
        for edge in edges:
            a, b = edge
            nodes.add(a)
            nodes.add(b)
            if self.__debug:
                bar.update(bar.value + 1)
        if self.__debug:
            bar.finish()
        net.add_nodes(list(nodes))

        if self.__debug:
            bar = ProgressBar(maxval=len(edges))
            bar.print('Preparando arestas...')
        self.set_status('Processing network edges.')
        max_weight = max(weights)
        min_weight = min(weights)

        for edge, weight, label in zip(edges, edge_widths, labels):
            options = {'weight': weight}
            if min_weight != max_weight:
                options['value'] = weight
            if show_labels:
                options['label'] = label
            net.add_edge(edge[0], edge[1], **options)
            if self.__debug:
                bar.update(bar.value + 1)
        if self.__debug:
            bar.finish()

        nodes_a = []
        nodes_b = []
        for edge in edges:
            a, b = edge
            nodes_a.append(a)
            nodes_b.append(b)
        result = {
            'table': {
                'node_a': nodes_a,
                'node_b': nodes_b,
                'label': labels,
                'weight': edge_widths,
            },
            'info': {
                'edges_number': len(edges),
                'nodes_number': len(nodes),
                'labels': set(labels),
            },
            'html': net.generate_html(),
        }

        if self.__work_directory is not None:
            with open(path.join(self.__work_directory, f'network_{suffix}.pkl'), 'wb') as file:
                pickle.dump(result, file)

        return result

        # figure = plt.figure(figsize=(12, 8))
        # for i, edge in enumerate(edges):
        #     a, b = edge
        #     self.__G.add_edge(a, b, label=labels[i], weight=edge_widths[i])

        # pos = nx.spring_layout(self.__G)

        # edge_alphas = [float(w) / max(weights) for w in weights]

        # cmap = plt.cm.winter
        # norm = plt.Normalize(vmin=min(weights), vmax=max(weights))
        # edge_colors = [cmap(norm(w)) for w in weights]

        # nx.draw(
        #     self.__G,
        #     pos,
        #     edge_color=edge_colors,
        #     font_color='orange',
        #     font_size=7,
        #     node_size=50,
        #     with_labels=True,  # rótulo do nó
        #     width=edge_widths,
        # )
        # sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        # sm.set_array([])
        # plt.colorbar(sm, label='Edge\'s weights', ax=plt.gca(), shrink=0.2)

        # nx.coloring.greedy_color(self.__G, strategy="largest_first")
        # edge_labels = dict([((u, v,), d['label']) for u, v, d in self.__G.edges(data=True)])
        # nx.draw_networkx_edge_labels(
        #     self.__G, pos,
        #     edge_labels=edge_labels if show_labels else {},
        #     font_color='navy',
        #     font_size=7,
        #     # label_pos=0.3,
        # )

        # file = NamedTemporaryFile(dir=self.__temp_directory)
        # plt.savefig(file, format='svg')

        # return file=

    def generate_network_from_csv(self, csv_file: str, column_key_name: str, edges_width_factor: float = 1,
                                  show_labels: bool = True, generate_entire_network: bool = False) -> dict:
        data = dict()
        with open(csv_file, 'r') as input_file:
            data_file = reader(input_file, delimiter=',', quotechar='"')
            columns = next(data_file)
            for line in data_file:
                record = dict(zip(columns, line))
                accession = record[column_key_name]
                if self.__nodes_index is not None:
                    accession = self.__nodes_index.index(accession)
                if accession not in data:
                    data[accession] = dict()
                for column in record:
                    if column != column_key_name:
                        value = record[column]
                        if value is not None and value.strip() not in NONE_VALUES:
                            if self.__nodes_index is not None:
                                value = self.__table_index[column].index(value)
                                column = self.__columns_index.index(column)
                            if column not in data[accession]:
                                data[accession][column] = {value}
                            else:
                                data[accession][column] |= {value}
                if len(data[accession]) == 0:
                    del data[accession]

        if self.__work_directory is not None:
            with open(path.join(self.__work_directory, f'data_dict.pkl'), 'wb') as file:
                pickle.dump(data, file)

        return self.generate_network_from_dict(
            data=data, columns=columns, column_key_name=column_key_name, edges_width_factor=edges_width_factor,
            show_labels=show_labels, generate_entire_network=generate_entire_network)

    # def generate_network_from_dataframe(self, dataframe: DataFrame, column_key_name: str, edges_width_factor: float = 1,
    #                                     show_labels=True) -> dict:
    #     dataframe = dataframe.replace(NONE_VALUES, np.NaN)
    #     columns = [i for i in dataframe.columns]
    #
    #     if self.__debug:
    #         bar = ProgressBar(maxval=len(dataframe) * len(columns) - 1)
    #         bar.print(f'Verificando dataset...')
    #
    #     data = dict()
    #     for column in columns:
    #         if column != column_key_name:
    #             dataframe_recorte = dataframe[pd.notna(dataframe[column])][[column_key_name, column]]
    #             for i, row in dataframe_recorte.iterrows():
    #                 temp = {}
    #                 if column != column_key_name and row[column] is not None:
    #                     temp[column] = {row[column]}
    #                 if temp:
    #                     if row[column_key_name] in data:
    #                         for column in temp:
    #                             if column in data[row[column_key_name]]:
    #                                 temp[column] = temp[column] | data[row[column_key_name]][column]
    #                     if row[column_key_name] not in data:
    #                         data[row[column_key_name]] = {}
    #                     data[row[column_key_name]][column] = temp[column]
    #                 if self.__debug:
    #                     bar.update(bar.value + 1)
    #             if self.__debug:
    #                 position = len(dataframe[column]) - len(dataframe_recorte)
    #                 if position > 0:
    #                     bar.update(position + 1)
    #     if self.__debug:
    #         bar.finish()
    #
    #     return self.generate_network_from_dict(data=data, columns=columns, column_key_name=column_key_name,
    #                                            edges_width_factor=edges_width_factor, show_labels=show_labels)

    def generate_network_from_dict(
            self, data: dict, columns: list, column_key_name: str, edges_width_factor: float = 1,
            show_labels: bool = True, generate_entire_network: bool = False) -> dict:

        if self.__nodes_index is not None:
            columns = [self.__columns_index.index(i) for i in columns]

        def __get_edges(_a, _b, _value_a, _value_b, _columns):
            if _value_a is None or _value_b is None:
                return None
            _temp = set()
            for _column in _columns:
                if _column != column_key_name:
                    for temp_a in _value_a.get(_column, {}):
                        for temp_b in _value_b.get(_column, {}):
                            # ignorando strings vazias
                            if temp_a is None:
                                continue
                            if isinstance(temp_a, str) and temp_a.strip() == '':
                                continue
                            if temp_a == temp_b:
                                _temp.add((_column, temp_a))
            if _temp:
                return _a, _b, _temp
            else:
                return None

        edges = {}
        if self.__debug:
            bar = ProgressBar(maxval=comb(len(data), 2))
            bar.print('Preparando dados para rede...')
        self.set_status('Processing network data.')
        if self.__debug:
            bar.print('Calculando combinações...')
        with NamedTemporaryFile(mode='w+', delete=True, dir=self.__temp_directory) as temp_file_network:
            for a, b in get_combinations([i for i in data.keys()]):
                if self.__nodes_index is not None:
                    a = str(a)
                    b = str(b)
                temp_file_network.write(' '.join([a, b]))
                temp_file_network.write('\n')
                if self.__debug:
                    bar.update(bar.value + 1)
            if self.__debug:
                bar.finish()
            if self.__debug:
                bar = ProgressBar(maxval=comb(len(data), 2))
                bar.print('Calculando arestas...')
                bar.update(0)
            temp_file_network.seek(0)
            for row in temp_file_network:
                a, b = row.strip().split(' ')
                if self.__nodes_index is not None:
                    a = int(a)
                    b = int(b)
                item = __get_edges(a, b, data.get(a, None), data.get(b, None), columns)
                if item is not None:
                    _a, _b, _edges = item
                    edges[(_a, _b)] = _edges  # usar arquivo para guardas essas arestas
                if self.__debug:
                    bar.update(bar.value + 1)
            if self.__debug:
                bar.finish()

        communities = self.get_communities(edges=edges)
        communities_count = len(communities)

        calculated_edges = self.calculate_edges(edges=edges, communities=communities)
        del communities
        del edges
        edges_all = calculated_edges['edges_all']
        labels_all = calculated_edges['labels_all']
        weights_all = calculated_edges['weights_all']

        edges_communities = calculated_edges['edges_communities']
        labels_communities = calculated_edges['labels_communities']
        weights_communities = calculated_edges['weights_communities']

        network_communities = []
        for i in range(0, communities_count):
            if self.__debug:
                self.set_status(f'Computing network community {i + 1} of {communities_count}...')
            temp_network = self.get_network(
                edges=edges_communities[i],
                labels=labels_communities[i],
                weights=weights_communities[i],
                edges_width_factor=edges_width_factor,
                show_labels=show_labels,
                suffix=i
            )
            temp_file_network = NamedTemporaryFile(delete=True, dir=self.__temp_directory)
            self.__temp_files.append(temp_file_network)
            with open(temp_file_network.name, 'wb') as tf:
                pickle.dump(temp_network, tf)
            network_communities.append(temp_file_network.name)

        if self.__debug:
            self.set_status(f'Computing entire network...')

        results = dict(communities=network_communities)
        if generate_entire_network:
            temp_network = self.get_network(
                edges=edges_all,
                labels=labels_all,
                weights=weights_all,
                edges_width_factor=edges_width_factor,
                show_labels=show_labels,
                suffix='all'
            )
            temp_file_network = NamedTemporaryFile(delete=True, dir=self.__temp_directory)
            self.__temp_files.append(temp_file_network)
            with open(temp_file_network.name, 'wb') as tf:
                pickle.dump(temp_network, tf)
            results['network'] = temp_file_network.name

        return results

    def plot_from_csv_file(self, csv_file: str, edges_width_factor: float = 4.5, show_labels: bool = True,
                           generate_entire_network: bool = False):
        edges = {}
        with open(csv_file, 'r') as file:
            csv_reader = reader(file, delimiter=',', quotechar='"')
            header = next(csv_reader)
            for row in csv_reader:
                row = dict(zip(header, row))
                connection = (row['node_a'], row['node_b'])
                if connection not in edges:
                    edges[connection] = set()
                edges[connection].add(row['label'])

        communities = self.get_communities(edges=edges)
        communities_count = len(communities)

        calculated_edges = self.calculate_edges(edges=edges, communities=communities)
        del edges
        del communities
        edges_all = calculated_edges['edges_all']
        labels_all = calculated_edges['labels_all']
        weights_all = calculated_edges['weights_all']

        edges_communities = calculated_edges['edges_communities']
        labels_communities = calculated_edges['labels_communities']
        weights_communities = calculated_edges['weights_communities']

        network_communities = []
        for i in range(0, communities_count):
            if self.__debug:
                self.set_status(f'Computing network community {i + 1} of {communities_count}...')
            temp_result = self.get_network(
                edges=edges_communities[i],
                labels=labels_communities[i],
                weights=weights_communities[i],
                edges_width_factor=edges_width_factor,
                show_labels=show_labels,
                suffix=i
            )
            temp_file_network = NamedTemporaryFile(delete=True, dir=self.__temp_directory)
            self.__temp_files.append(temp_file_network)
            with open(temp_file_network.name, 'wb') as tf:
                pickle.dump(temp_result, tf)
            network_communities.append(temp_file_network.name)

        if self.__debug:
            self.set_status(f'Computing entire network...')
        results = dict(
            communities=network_communities
        )
        if generate_entire_network:
            network_all = self.get_network(
                edges=edges_all,
                labels=labels_all,
                weights=weights_all,
                edges_width_factor=edges_width_factor,
                show_labels=show_labels,
                suffix='all'
            )
            temp_file_network = NamedTemporaryFile(delete=True, dir=self.__temp_directory)
            self.__temp_files.append(temp_file_network)
            with open(temp_file_network.name, 'wb') as tf:
                pickle.dump(network_all, tf)
            results['network'] = temp_file_network.name

        return results

    def set_status(self, message: str):
        if self.__status_callback is not None:
            self.__status_callback(message)
            print(message)
        else:
            print(message)
