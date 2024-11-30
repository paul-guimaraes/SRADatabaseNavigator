from copy import deepcopy
from datetime import datetime
from nltk import ngrams
from nltk.stem import WordNetLemmatizer
from nltk.tree import Tree
from os import makedirs
from os import path
from pathlib import Path
from threading import Lock
from urllib.request import urlretrieve
import logging
import pickle


class Mesh:
    def __init__(self):
        self.__descriptors = {}
        self.__qualifiers = {}
        self.__supplementary_concept_records = {}

        self.__lemma_terms = {
            'c': {},
            'd': {},
            'q': {},
        }

        # sem espaços
        self.__no_space_terms = {
            'c': {},
            'd': {},
            'q': {},
        }

        self.__mesh_categories = {
            'c': 'MESH SUPPLEMENTARY',  # 'SUPPLEMENTARY CONCEPT RECORD',
            'd': 'MESH DESCRIPTOR',  # 'DESCRIPTOR DATA ELEMENT',
            'q': 'MESH QUALIFIER',  # 'QUALIFIER DATA ELEMENT'
        }
        self.__mesh_files = {
            'c': self.__supplementary_concept_records,
            'd': self.__descriptors,
            'q': self.__qualifiers
        }
        self.__mesh_keys = {
            'c': 'NM',
            'd': 'MH',
            'q': 'SH'
        }
        self.__mesh_keys_names = {
            'c': 'Name of Substance',
            'd': 'MeSH Heading',
            'q': 'Subheading'
        }
        self.__mesh_max_sizes = {
            'c': 0,
            'd': 0,
            'q': 0,
        }
        self.__mesh_min_sizes = {
            'c': -1,
            'd': -1,
            'q': -1,
        }
        self.__mesh_synonyms_values = {
            'c': 'SY',
            'd': 'ENTRY',
            'q': 'SH'
        }
        # self.stemmer = PorterStemmer()
        self.lemmatizer = WordNetLemmatizer()
        
    # marca termos do mesh em uma sentença
    def chunk(self, sentence):
        sentence = deepcopy(sentence)
        test_sentence = []
        test_sentence_word_sizes = {}
        for i, item in enumerate(sentence):
            if isinstance(item, Tree):
                for node in item:
                    word, tag = node
                    test_sentence.append(word)
                test_sentence_word_sizes[i] = len(item)
            else:
                word, tag = item
                test_sentence.append(word)
                test_sentence_word_sizes[i] = 1
        localized_terms = []
        punctuations = ',.-:()[]{}-_'
        for category in ('d', 'q', 'c'):  # self.__mesh_files
            max_size = len(test_sentence)
            if max_size > self.__mesh_max_sizes[category]:
                max_size = self.__mesh_max_sizes[category]
            for size in range(max_size, self.__mesh_min_sizes[category]-1, -1):
                for first_word_id, ngram in enumerate(ngrams(test_sentence, size)):
                    term = ' '.join(ngram).lower()
                    term = term.replace('( ', '(')
                    term = term.replace(' )', ')')
                    term = term.replace('[ ', '[')
                    term = term.replace(' ]', ']')
                    for punctuation in punctuations:
                        term = term.replace(f' {punctuation}', punctuation)
                    lemmatized_term = self.lemmatizer.lemmatize(term)
                    if (term in self.__mesh_files[category]
                            or (lemmatized_term in self.__lemma_terms[category]
                                and self.__lemma_terms[category][lemmatized_term] in self.__mesh_files[category])):
                        last_word_id = first_word_id + size

                        category_name = f'{self.__mesh_files[category][term][self.__mesh_keys[category]] if term in self.__mesh_files[category] else self.__mesh_files[category][self.__lemma_terms[category][lemmatized_term]][self.__mesh_keys[category]]}'
                        category_name = f'{self.__mesh_keys_names[category].upper()}: {category_name}'
                        localized_term = {
                            'first_word_id': first_word_id,
                            'last_word_id': last_word_id,
                            'size': size,
                            # 'category': self.__mesh_categories[category],
                            'category': category_name,
                            'term': term
                        }
                        add = True
                        for temp in localized_terms:
                            if first_word_id >= temp['first_word_id'] and last_word_id <= temp['last_word_id']:
                                add = False
                                break
                        if add:
                            localized_terms.append(localized_term)
                        # else:
                        #     print('Ignoring', localized_term)  # TODO: verificar essa situação
        # TODO: verificar Entidades já localizadas antes de incluir uma nova
        for localized_term in sorted(localized_terms, key=lambda x: x['first_word_id'], reverse=True):
            true_start = 0
            for i in sorted(test_sentence_word_sizes):
                if sum([value for key, value in test_sentence_word_sizes.items() if key <= i]) - 1 >= localized_term['first_word_id']:
                    true_start = i
                    break
            if isinstance(sentence[true_start], Tree):
                if len(sentence[true_start]) == localized_term['size']:
                    sentence[true_start].set_label(localized_term['category'])
                elif len(sentence[true_start]) > localized_term['size']:
                    tree = Tree(localized_term['category'], sentence[true_start][0:localized_term['size']])
                    del sentence[true_start][0:localized_term['size']]
                    sentence[true_start].insert(0, tree)
                else:  # TODO len(sentence[true_start]) < localized_term['size']
                    temp = []
                    current_position = true_start
                    while len(temp) < localized_term['size']:
                        if isinstance(sentence[current_position], Tree):
                            for item in sentence[current_position]:
                                if len(temp) < localized_term['size']:
                                    temp.append(item)
                        else:
                            temp.append(sentence[current_position])
                        current_position += 1
                    tree = Tree(localized_term['category'], temp)
                    sentence[true_start] = tree
                    for i in range(current_position-1, true_start, -1):
                        del sentence[i]
            else:
                tree = Tree(localized_term['category'], sentence[true_start:true_start+localized_term['size']])

                def get_node_count(_item, length=0):
                    if isinstance(_item, Tree):
                        result = 0
                        for _j, _k in enumerate(_item):
                            result += get_node_count(_k, length + _j)
                        return result
                    else:
                        return 1

                i = 0
                j = true_start
                del_indexes = []
                while i < localized_term['size']:
                    del_indexes.append(j)
                    i += get_node_count(sentence[j])
                    j += 1
                for i in reversed(del_indexes):
                    del sentence[i]
                # for i in range(true_start+localized_term['size']-1, true_start-1, -1):
                #     print('====>', localized_term)
                #     del sentence[i]
                sentence.insert(true_start, tree)
        return sentence

    # retorna as entradas do mesh para um termo qualquer
    def find(self, term: str, term_type: str = 'd'):
        term = term.lower()
        if term_type not in self.__mesh_files:
            raise Exception(f'Invalid term type. Please use {" or ".join(self.__mesh_files.keys())} parameter{"s" if len(self.__mesh_keys) > 1 else ""}')
        result = self.__mesh_files[term_type][term] if term in self.__mesh_files[term_type] else None
        if result is None:
            no_space_term = term.lower().replace(' ', '')
            result = self.__mesh_files[term_type][self.__no_space_terms[term_type][no_space_term]] if no_space_term in self.__no_space_terms[term_type] else None
        if result is None:
            lemma_term = self.lemmatizer.lemmatize(term)
            if lemma_term.lower() != term.lower():
                result = self.__mesh_files[term_type][self.__lemma_terms[term_type][lemma_term]] if term in self.__mesh_files[term_type] else None
        return result

    def find_term_and_category(self, term: str, use_ngrams: bool = False, cut_words: bool = True):
        results = []
        for category in ('d', 'q', 'c'):
            result = self.find(term=term, term_type=category)
            if not result:
                synonyms = self.get_synonym(term)
                for synonym in synonyms:
                    if synonym.lower() != term.lower():
                        result = self.find(term=synonym, term_type=category)
                        if result:
                            break
            if not result:
                # checking hyphen word.
                if ' ' in term:
                    new_term = term.replace(' ', '-')
                    result = self.find(term=new_term, term_type=category)
            if result:
                results.append(dict(term_type=self.__mesh_keys_names[category], mesh_term=result[self.__mesh_keys[category]]))
            else:
                if use_ngrams:
                    ngram_terms = []
                    splitted_term = term.split(' ')
                    if len(splitted_term) > 1:
                        max_size = len(splitted_term) - 1
                        if max_size > self.__mesh_max_sizes[category]:
                            max_size = self.__mesh_max_sizes[category]
                        for size in range(max_size, 0, -1):
                            # found = False  # TODO: interromper se achar o primeiro?
                            for i, ngram in enumerate(ngrams(term.split(' '), size)):
                                ngram = ' '.join(ngram)
                                temp = self.find(term=ngram, term_type=category)
                                if not temp:
                                    synonyms = self.get_synonym(ngram)
                                    for synonym in synonyms:
                                        if synonym.lower() != ngram.lower():
                                            temp = self.find(term=synonym, term_type=category)
                                            # if temp:
                                            #     break  # TODO: interromper?
                                if not temp:
                                    # checking hyphen word.
                                    temp = self.find(term=ngram.replace(' ', '-'), term_type=category)
                                if temp:
                                    for temp_ngram in ngram_terms:
                                        if temp_ngram['start'] <= i <= temp_ngram['start'] + temp_ngram['size'] - 1 and i + size - 1 > temp_ngram['start'] + temp_ngram['size'] - 1:
                                            break  # FIXME: tratar caso em que dois ngramas começarem na mesma região mas o segundo terminar depois do primeiro.
                                        elif temp_ngram['start'] <= i <= temp_ngram['start'] + temp_ngram['size'] - 1:
                                            break
                                    else:
                                        ngram_terms.append(dict(
                                            term_type=self.__mesh_keys_names[category],
                                            mesh_term=temp[self.__mesh_keys[category]],
                                            size=size,
                                            start=i,
                                            term=ngram,
                                        ))
                                    # found = True  # TODO: interromper se achar o primeiro?
                            #     if found:  # TODO: interromper se achar o primeiro?
                            #         break
                            # if found:  # TODO: interromper se achar o primeiro?
                            #     break
                    if ngram_terms:
                        new_term = []
                        for i in range(0, len(splitted_term)):
                            new_term.insert(i, splitted_term[i])
                        for ngram in ngram_terms:
                            for i in range(ngram['start'], ngram['start'] + ngram['size']):
                                new_term[i] = None
                            new_term[ngram['start']] = ngram['mesh_term']
                        new_term = ' '.join([i for i in new_term if i is not None])
                        results.append(
                            dict(
                                term_type=self.__mesh_keys_names[category],
                                mesh_term=new_term,
                                ngram_terms=ngram_terms
                            )
                        )
                    elif cut_words:
                        pass  # corrigir e implementar.
                        # # cortando a palavra para verifica se ela possui termos combinados
                        # size = 4  # verificar se esse tamanho está bom.
                        # if len(term) > size:
                        #     temp_terms = []
                        #     ngram_terms = []
                        #     for limit in range(len(term)-1, size-1, -1):
                        #         for start in range(0, len(term)):
                        #             end = start + limit
                        #             if end <= len(term):
                        #                 temp = term[start:end]
                        #                 temp_term = []
                        #                 if start == 0:
                        #                     temp_term.append(temp)
                        #                 else:
                        #                     temp_term.append(term[0:start])
                        #                     temp_term.append(temp)
                        #                 if end < len(term):
                        #                     temp_term.append(term[end:])
                        #                 if temp_term not in temp_terms:
                        #                     temp_terms.append(temp_term)
                        #                     for temp in temp_term:
                        #                         print(temp, temp_term)
                        #                         temp_mesh = self.find(term=temp, term_type=category)
                        #                         if temp_mesh:
                        #                             ngram = dict(
                        #                                 term_type=self.__mesh_keys_names[category],
                        #                                 mesh_term=temp_mesh[self.__mesh_keys[category]],
                        #                                 size=len(temp),
                        #                                 start=start,
                        #                                 term=temp,
                        #                             )
                        #                             for j, ngram_term in enumerate(ngram_terms):
                        #                                 if ngram_term['size'] == ngram['size'] or ngram_term['size'] == ngram['size'] + 1:
                        #                                     ngram_terms[j] = ngram
                        #                                     break
                        #                             else:
                        #                                 ngram_terms.append(ngram)
                        #     if ngram_terms:
                        #         new_term = []
                        #         for i in term:
                        #             new_term.append(i)
                        #         for ngram in reversed(ngram_terms):
                        #             for i in range(ngram['start'], ngram['start'] + ngram['size']):
                        #                 new_term[i] = None
                        #             new_term.insert(ngram['start'], ' ')
                        #             for i in reversed(ngram['mesh_term']):
                        #                 new_term.insert(ngram['start'], i)
                        #         new_term = ''.join([i for i in new_term if i is not None]).strip()
                        #         results.append(
                        #             dict(
                        #                 term_type=self.__mesh_keys_names[category],
                        #                 mesh_term=new_term,
                        #                 ngram_terms=ngram_terms
                        #             )
                        #         )
        return results

    def get_synonym(self, term: str, term_type=None):
        if term_type is None:
            mesh_values = [self.find(term, term_type=category) for category in ('d', 'q', 'c')]
        else:
            mesh_values = [self.find(term, term_type=term_type)]

        def get_synonym(_mesh_value, _synonyms):
            key_name = self.__mesh_synonyms_values[_mesh_value['RECTYPE'].lower()]
            if key_name in _mesh_value:
                with Lock():
                    if isinstance(_mesh_value[key_name], str):
                        _synonyms.add(_mesh_value[key_name])
                    else:
                        for entry in _mesh_value[key_name]:
                            _synonyms.add(entry.lower())

        synonyms = set()
        for mesh_value in mesh_values:
            if mesh_value is not None:
                get_synonym(mesh_value, synonyms)  # TODO: colocar numa thread
        # with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        #     futures = []
        #     for mesh_value in mesh_values:
        #         if mesh_value is not None:
        #             futures.append(executor.submit(_get_synonym, mesh_value, synonyms))
        #     for future in as_completed(futures):
        #         if future.exception():
        #             raise Exception(future.exception())
        return tuple(synonyms)

    # carrega os dados de um arquivo de descritores do MESH no formato ASCII
    def load_data(self):
        # year = datetime.now().year
        year = 2024
        # mesh_url = 'https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/asciimesh/'  # TODO: implementar mecanismo de atualização por ano
        mesh_url = f'https://nlmpubs.nlm.nih.gov/projects/mesh/{year}/asciimesh/'
        mesh_directory = path.join(Path.home(), '.data', 'mesh')
        if not path.exists(mesh_directory):
            makedirs(mesh_directory)
        mesh_dictionary = path.join(mesh_directory, 'mesh_dictionary.p')

        if path.exists(mesh_dictionary) and path.getsize(mesh_dictionary) > 0:
            with open(mesh_dictionary, 'rb') as mesh_dictionary:
                self.__dict__.update(pickle.load(mesh_dictionary))
        else:
            with open(mesh_dictionary, 'wb') as mesh_dictionary:
                for category in self.__mesh_files:
                    mesh_file = path.join(mesh_directory, f'{category}{year}.bin')
                    if not path.exists(mesh_file):
                        logging.info(f'Download MeSH {category} data...')
                        if not path.exists(mesh_directory):
                            makedirs(mesh_directory)
                        urlretrieve(f'{mesh_url}{category}{year}.bin', mesh_file)
                for category in self.__mesh_files:
                    mesh_file = path.join(mesh_directory, f'{category}{year}.bin')
                    with open(mesh_file, 'r') as file:
                        record = None
                        for line in file:
                            line = line.strip()
                            if line == '*NEWRECORD':
                                record = {}
                            elif len(line) == 0 and record is not None:
                                self.__mesh_files[category][record[self.__mesh_keys[category]].lower()] = record
                                for key_name in ('ENTRY', 'SY'):
                                    if key_name in record:
                                        if isinstance(record[key_name], str):
                                            entries = [record[key_name]]
                                        else:
                                            entries = record[key_name]
                                        key_name = 'PRINT ENTRY'
                                        if key_name in record:
                                            if isinstance(record[key_name], str):
                                                entries.append(record[key_name])
                                            else:
                                                entries.extend(record[key_name])
                                        entries = set(entries)
                                        for key in entries:
                                            key = key.lower()
                                            if key != record[self.__mesh_keys[category]] \
                                                    and key not in self.__mesh_files[category]:
                                                self.__mesh_files[category][key] = record
                                record = None
                            else:
                                line = [i.strip() for i in line.split('=', 1)]
                                if len(line) == 2:
                                    key, value = line
                                    if '|' in value:
                                        temp = value.split('|')
                                        if len(temp[-1])+1 == len(temp):
                                            value = temp[temp[-1].index('a')]
                                    if key not in record:
                                        record[key] = value
                                    else:
                                        if isinstance(record[key], str):
                                            record[key] = [record[key]]
                                        record[key].append(value)
                    for term in self.__mesh_files[category]:
                        size = len(term.split(' '))
                        if size > self.__mesh_max_sizes[category]:
                            self.__mesh_max_sizes[category] = size
                        if self.__mesh_min_sizes[category] < 0:
                            self.__mesh_min_sizes[category] = size
                        elif size < self.__mesh_min_sizes[category]:
                            self.__mesh_min_sizes[category] = size

                        stem_term = ' '.join(self.lemmatizer.lemmatize(i) for i in term.replace(',', '').split(' '))
                        self.__lemma_terms[category][stem_term] = term
                        no_space_term = term.lower().replace(' ', '')
                        self.__no_space_terms[category][no_space_term] = term

                pickle.dump(self.__dict__, mesh_dictionary)
