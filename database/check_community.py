from data.util import NONE_VALUES


def split_term(sentence):
    if len(sentence) > 0 and ':' in sentence:
        results = []
        while len(sentence) > 0:
            if ':' in sentence:
                start = 0
                end = sentence.index(':')
                while ',' in sentence[start:end]:
                    temp = sentence[start:end].rsplit(',')[0]
                    results[-1][1] += temp
                    sentence = sentence[len(temp) + 1:]
                    end = sentence.index(':')
                term = sentence[start:end]
                start = end+1
                end = sentence.index(',') if ',' in sentence else len(sentence)
                value = sentence[start:end]
                if end < len(sentence):
                    if '(' in value:
                        end = sentence.index(')')+1
                        end += sentence[end:].index(',') if ',' in sentence[end:] else len(sentence)
                        value = sentence[start:end]
                    if '[' in value:
                        end = sentence.index(']')+1
                        end += sentence[end:].index(',') if ',' in sentence[end:] else len(sentence)
                        value = sentence[start:end]
                    if '{' in value:
                        end = sentence.index('}')+1
                        end += sentence[end:].index(',') if ',' in sentence[end:] else len(sentence)
                        value = sentence[start:end]
                    if '"' in value:
                        end += sentence[end:].index('"')
                        end += sentence[end:].index(',') if ',' in sentence[end:] else len(sentence)
                        value = sentence[start:end]
                sentence = sentence[end + 1:].strip()

                results.append([term.strip(), value.strip()])
            else:
                results[-1][1] = f'{results[-1][1]}{sentence}'
                sentence = ''
        return results
    else:
        return [sentence]


if __name__ == '__main__':
    from argparse import ArgumentParser
    from os import listdir
    from os import makedirs
    from os import path
    import csv

    parser = ArgumentParser('Creates summary and extract fields from a directory of networks in csv format. (Fields: label,node_a,node_b,weight)')
    parser.add_argument('--input_directory', required=True)
    parser.add_argument('--output_directory', required=True)
    parser.add_argument('--summarize', required=False, action='store_true')
    parser.add_argument('--extract_fields', required=False, action='store_true')

    args = parser.parse_args()

    if args.extract_fields:
        print('Extracting fields from files...')
        for file in sorted(listdir(args.input_directory)):
            if file.endswith('.csv'):
                name = file.rsplit('.', maxsplit=1)[0]
                number = name.rsplit('_', maxsplit=1)
                number = number[1] if len(number) > 1 else ''
                print(f'Checking {file}...')
                with open(path.join(args.input_directory, file), 'r') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    attributes = set()
                    attributes_presents = {}
                    attributes_values = {}
                    for line in reader:
                        record = dict(zip(header, line))
                        if record['node_a'] not in attributes_presents:
                            attributes_presents[record['node_a']] = set()
                            attributes_values[record['node_a']] = {}
                        if record['node_b'] not in attributes_presents:
                            attributes_presents[record['node_b']] = set()
                            attributes_values[record['node_b']] = {}
                        for term, value in split_term(record['label']):
                            attributes.add(term)
                            if value not in NONE_VALUES:
                                attributes_presents[record['node_a']].add(term)
                                attributes_presents[record['node_b']].add(term)
                                if term not in attributes_values[record['node_a']]:
                                    attributes_values[record['node_a']][term] = set()
                                if term not in attributes_values[record['node_b']]:
                                    attributes_values[record['node_b']][term] = set()
                                attributes_values[record['node_a']][term].add(value)
                                attributes_values[record['node_b']][term].add(value)
                    if len(attributes) > 0:
                        if not path.exists(path.join(args.output_directory, name)):
                            makedirs(path.join(args.output_directory, name))
                        f.seek(0)
                        reader = csv.reader(f)
                        header = next(reader)
                        columns = sorted(attributes)
                        with open(path.join(args.output_directory, name, f'terms.csv'), 'w') as f2:
                            writer = csv.writer(f2)
                            writer.writerow(['node_a', 'node_b'] + columns)
                            for line in reader:
                                record = dict(zip(header, line))
                                values = {attribute: set() for attribute in attributes}
                                for term, value in split_term(record['label']):
                                    values[term].add(value)
                                writer.writerow([record['node_a'], record['node_b']] + ['; '.join(values[column]) for column in columns])
                        with open(path.join(args.output_directory, name, f'present_terms.csv'), 'w') as f2:
                            writer = csv.writer(f2)
                            writer.writerow(['node'] + columns)
                            for node in sorted(attributes_presents):
                                writer.writerow([node] + [column in attributes_presents[node] for column in columns])
                        with open(path.join(args.output_directory, name, f'present_values.csv'), 'w') as f2:
                            writer = csv.writer(f2)
                            writer.writerow(['node'] + columns)
                            for node in sorted(attributes_values):
                                record = [node]
                                for column in columns:
                                    record.append('; '.join(sorted(attributes_values[node][column])) if column in attributes_values[node] else '')
                                writer.writerow(record)

    if args.summarize:
        print('Summarizing...')
        data = []
        for file in sorted(listdir(args.input_directory)):
            if file.endswith('.csv'):
                name = file.rsplit('.', maxsplit=1)[0]
                number = name.rsplit('_', maxsplit=1)[1]
                print(f'Checking {file}...')
                with open(path.join(args.input_directory, file), 'r') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    attributes = set()
                    values = set()
                    nodes = set()
                    labels = set()
                    weights = set()
                    for line in reader:
                        record = dict(zip(header, line))
                        for term, value in split_term(record['label']):
                            attributes.add(term)
                            values.add(f'{term}:{value}')
                        nodes.add(record['node_a'])
                        nodes.add(record['node_b'])
                        labels.add(record['label'])
                        weights.add(record['weight'])
                    for value in values:
                        attributes.add(value.split(':')[0].strip())
                if len(attributes) > 0:
                    if not path.exists(path.join(args.output_directory, name)):
                        makedirs(path.join(args.output_directory, name))
                    for file_name, item in (('attributes', attributes), ('labels', labels), ('weights', weights),
                                            ('values', values)):
                        with open(path.join(args.output_directory, name, f'{file_name}.txt'), 'w') as f:
                            for record in sorted(item):
                                f.write(f'{record}\n')
                data.append((name, number, len(nodes), len(weights), len(labels), len(attributes), len(values)))
        if len(data) == 0:
            print('No communities found.')
        else:
            if not path.exists(args.output_directory):
                makedirs(args.output_directory)
            with open(path.join(args.output_directory, 'summary.csv'), 'w') as f:
                writer = csv.writer(f)
                writer.writerow(['Community', 'ID', 'Nodes', 'Weight', 'Connections', 'Attributes', 'Values'])
                for record in data:
                    writer.writerow(record)
