from collections import namedtuple
from concurrent.futures import as_completed, ProcessPoolExecutor
from multiprocessing import cpu_count
import lxml.etree
from threading import Lock
from os import path as ospath

import nltk
import pandas as pd
from re import sub
from concurrent.futures import ThreadPoolExecutor

from .mesh import Mesh
from .synonym import Synonym

from nltk import data, Tree, ParentedTree
from nltk import download
from .util import format_name
from nltk.stem import PorterStemmer
from nltk import pos_tag
from nltk import sent_tokenize
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.corpus import wordnet

from joblib import Parallel, delayed
from multiprocessing import cpu_count
import logging


class Mining:
    # atributo para garantir que os corpus só serão checados na primeira instância.
    downloaded_corpus = False

    def __init__(self):
        if Mining.downloaded_corpus is False:
            # verifying necessary libraries.
            with Parallel(n_jobs=-1, backend='loky') as parallel:
                Library = namedtuple('Library', 'path name')
                libraries = [
                    Library('chunkers/maxent_ne_chunker', 'maxent_ne_chunker'),
                    Library('corpora/omw-1.4', 'omw-1.4'),
                    Library('corpora/stopwords', 'stopwords'),
                    Library('corpora/wordnet', 'wordnet'),
                    Library('corpora/words', 'words'),
                    Library('taggers/averaged_perceptron_tagger_eng', 'averaged_perceptron_tagger_eng'),
                    Library('taggers/averaged_perceptron_tagger', 'averaged_perceptron_tagger'),

                    Library('tokenizers/punkt', 'punkt'),
                ]
                temp_version = nltk.__version__.split('.')
                if len(temp_version) > 2:
                    temp_version = temp_version[:2]
                temp_version = '.'.join(temp_version)
                temp_version = float(temp_version)
                if temp_version >= 3.6:
                    libraries.append(
                        Library('tokenizers/punkt_tab', 'punkt_tab'),
                    )

                def check_download(library):
                    try:
                        data.find(library.path)
                    except LookupError:
                        download(library.name)

                logging.info('Checking NLTK libraries...')
                results = parallel(delayed(check_download)(library) for library in libraries)
                Mining.downloaded_corpus = True

        logging.info('Loading NLTK models...')
        self.cache = {}
        self.lemmatizer = WordNetLemmatizer()
        self.stemmer = PorterStemmer()
        self.stop_words = set(stopwords.words('english'))

        logging.info('Loading synonyms...')
        self.synonym = Synonym()
        logging.info('Loading MeSH data...')
        self.mesh = Mesh()
        self.mesh.load_data()

    # TODO: melhorar, parece meio lento, pode ser por Big O grande
    def __get_counts_data_from_xml(self, xml: lxml.etree.ElementTree, path: str, tags: dict, cache: bool = False):
        temp_result = {path: {}}
        terms = set()
        for items in tags[path]:
            terms.add(items.text)
        terms = self.get_synonyms(terms, terms, cache=cache)
        searchs = []
        for term in terms:
            search = set()  # items para busca
            for item in terms[term]:
                search.add(item)
                if item is not None:
                    search.add(item)
            if search not in searchs:
                searchs.append(search)
                # colum_name = '/'.join(set([i.lower() for i in search]))
                colum_name = sorted(search)[0]
                if colum_name not in temp_result[path]:
                    temp_result[path][colum_name] = {'id': set(), 'category': {}}
                for tag in tags[path]:  # todo: aqui, continuar conversão
                    if tag.text in search:
                        parent = tag
                        while parent.getparent().tag != 'EXPERIMENT_PACKAGE':
                            parent = parent.getparent()
                        attribute = tag.getparent()
                        value = attribute.find('VALUE')
                        if value is not None:
                            synonym = None
                            synonyms = self.get_synonyms(value.text,
                                                         [i for i in temp_result[path][colum_name]['category']],
                                                         cache=cache)
                            if synonyms is not None:
                                for syn in synonyms:
                                    if syn in temp_result[path][colum_name]['category']:
                                        synonym = syn
                                        break
                            if synonym is None and value.text not in temp_result[path][colum_name]['category']:
                                temp_result[path][colum_name]['category'][value.text] = set()
                            temp_result[path][colum_name]['id'].add(xml.getelementpath(parent))
                            key = value.text if synonym is None else synonym
                            temp_result[path][colum_name]['category'][key].add(xml.getelementpath(parent))
        return temp_result[path]

    def get_counts_data_from_xml(self, xml: lxml.etree.ElementTree, path: str, cache: bool = False):
        tags = {}
        temp = xml.xpath(path)
        for tag in temp:
            node = sub('\[.*?\]', '', xml.getelementpath(tag))
            if node not in tags:
                tags[node] = set()
            tags[node].add(tag)

        results = {}
        with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
            futures = {}
            for path in tags:
                futures[executor.submit(self.__get_counts_data_from_xml, xml, path, tags, cache)] = path
            for future in as_completed(futures):
                if future.exception():
                    raise future.exception()
                results[futures[future]] = future.result()
        return results

    def get_counts_file_from_xml(self, xml: lxml.etree.ElementTree, path: str, output_dir, cache: bool = False):
        results = self.get_counts_data_from_xml(xml, path, cache=cache)
        for path in results:
            base_path = path.split('/')[1]
            dataframe_consolidado = None
            with pd.ExcelWriter(ospath.join(output_dir, f'{base_path}.xlsx')) as writer:
                for tag in sorted(results[path]):
                    formatted_tag = []
                    for temp in sorted(tag.split('/')):
                        if temp not in formatted_tag:
                            if temp.replace('_', ' ') not in formatted_tag:
                                formatted_tag.append(temp)
                    formatted_tag = '/'.join(formatted_tag)
                    index = []
                    values = []
                    for value in results[path][tag]['category']:
                        index.append(value)
                        values.append(len(results[path][tag]['category'][value]))
                    dataframe = pd.DataFrame({formatted_tag: values}, index=index)
                    dataframe = dataframe.sort_index()
                    total = pd.DataFrame({formatted_tag: [len(results[path][tag]['id'])]}, index=['total'])
                    dataframe = dataframe.append(total)
                    dataframe.to_excel(writer, sheet_name=format_name(formatted_tag)[:30])
                    index = []
                    values = []
                    for category in results[path][tag]['category']:
                        for _id in results[path][tag]['category'][category]:
                            index.append(_id)
                            values.append(category)
                    dataframe = pd.DataFrame(data={formatted_tag: values}, index=index)
                    if dataframe_consolidado is None:
                        dataframe_consolidado = dataframe
                    else:
                        dataframe_consolidado = dataframe_consolidado.merge(dataframe, how='outer', left_index=True,
                                                                            right_index=True)
            dataframe_consolidado.index.name = 'accession'
            index = {}
            for path in dataframe_consolidado.index:
                index[path] = xml.find(path).get('accession')
            dataframe_consolidado.rename(index=index).to_excel(ospath.join(output_dir, f'{base_path}_consolidado.xlsx'))

    @staticmethod
    def get_entities_from_tree(tree):
        tree = ParentedTree.convert(tree)
        Entity = namedtuple('Entity', 'absolute_position label leaves position')
        results = []
        for subtree in tree.subtrees():
            if subtree.label() != 'S':
                position = []
                if subtree in tree:
                    position.append(tree.index(subtree))
                else:
                    parent = subtree
                    while parent.parent():
                        position.insert(0, parent.parent().index(parent))
                        parent = parent.parent()
                position = tuple(position)
                subtree = Entity(
                    absolute_position=sum(position),
                    label=subtree.label(),
                    leaves=subtree.leaves(),
                    position=position,
                )
                results.append(subtree)
        return tuple(results)

    def get_mesh_term(self, term: str):
        self.mesh

    def get_pos_tag(self, text: str, entity: bool = False):  # TODO: terminar
        # text preprocessing: split sentences, tokenize and pos tagging.
        # tag set list on https://www.ling.upenn.edu/courses/Fall_2003/ling001/penn_treebank_pos.html
        # or can obtainned on nltk.help.{brown,claws5,upenn_tagset}()
        # nltk.help.upenn_tagset()
        text = text.replace('\n', '').strip()
        text = sent_tokenize(text)
        text = [word_tokenize(sentence) for sentence in text]
        for i, sentence in enumerate(text):
            text[i] = [word for word in sentence if word not in self.stop_words]
        text = [pos_tag(sentence) for sentence in text]
        if entity:
            text = [Tree('S', sentence) for sentence in text]
            # text = [chunk.ne_chunk(sentence) for sentence in text]  # TODO: ver necessidade disso aqui
            text = [self.mesh.chunk(sentence) for sentence in text]
        return text

    def get_synonyms(self, subject, query=None, cache: bool = False, remove_inverted_repeated: bool = True):
        """Procura por sinônimos de uma dada expressão.
        Se o parâmetro words for None, retorna todos os sinônimos. Caso contrarío, retorna somente sinônimos que
        existem no conjunto words.

        :param cache: Se verdadeiro, utiliza um cache para todas as consultas da instância.
        :param subject: String contendo a palavra de origem ou uma coleção de palavras.
        :param query: Conjunto de palavras a serem comparadas com a palavra de origem.
        :param remove_inverted_repeated: Se verdadeiro, mantem apenas o primeira correspondência para termos invertidos
        iguais. Exemplo: se a = b e b = a, mantém no retorno apenas a = b.
        :return: retorna um conjunto de sinônimos ou None quando não houver nenhum.
        """
        # TODO considerar usar hypernyms()
        if subject is None:
            return None  # TODO checar isso aqui, podem haver complicações.
        # normalizando todas as palavras em minúsculo
        if isinstance(subject, str):
            subject = subject.lower()
        else:
            subject = set((word.lower() for word in subject))
            remove = set()
            for word in sorted(subject):
                if ' ' in word:
                    teste = word.replace(' ', '_')
                    if teste in subject:
                        remove.add(teste)
            for word in remove:
                subject.remove(word)
        if query is not None:
            if isinstance(query, str):
                query = query.lower()
            else:
                query = set((word.lower() for word in query))
        results = {}
        temp_query = None
        if query is not None:
            temp_query = {format_name(word) for word in query}

        def get_synonyms_sep_1(_word, _query, _temp_query, _remove_inverted_repeated, _results, _cache,
                               _check_reverse=True, _check_indirect_synonyms=True):
            if _word not in _results:
                _reverserd_word = _word
                if ' ' in _reverserd_word:
                    _reverserd_word = ' '.join(_reverserd_word.split(' ')[::-1])
                elif '_' in _reverserd_word:
                    _reverserd_word = '_'.join(_reverserd_word.split('_')[::-1])
                synonyms = set()
                w = _word.replace(' ', '_')
                while '__' in w:
                    w = w.replace('__', '_')
                rw = '_'.join(w.split('_')[::-1])
                if _query is not None:
                    for i in _query:
                        q = i.replace(' ', '_')
                        while '__' in q:
                            q = q.replace('__', '_')
                        if w == q:
                            synonyms.add(i)
                        elif w.replace('_', '') == q.replace('_', '') or rw.replace('_', '') == q.replace('_', ''):
                            synonyms.add(i)

                if _cache and 'synsets' not in self.cache:
                    with Lock():
                        self.cache['synsets'] = {}
                formatted_word = format_name(_word)
                if _cache and formatted_word in self.cache['synsets']:
                    synsets = self.cache['synsets'][formatted_word]
                else:
                    with Lock():
                        try:
                            synsets = wordnet.synsets(formatted_word)
                            if _cache:
                                self.cache['synsets'][formatted_word] = synsets
                        except Exception as e:
                            print(formatted_word, str(e))
                            synsets = []
                for syn in synsets:
                    if syn is not None:
                        for lema in syn.lemmas():
                            if (_query is None
                                    or (lema.name() in _temp_query or lema.name().replace('_', ' ') in _temp_query)):
                                if _query is not None:
                                    if lema.name() in _query:
                                        synonyms.add(lema.name())
                                else:
                                    synonyms.add(lema.name())
                                if _query is not None:
                                    for item in _query:
                                        if item != lema.name() and format_name(item) == lema.name():
                                            synonyms.add(item)
                # synonyms with lemmatization and stemming
                for _type in (
                        'lemmatize',
                        # 'stemming'  # TODO Pensar se devo usar stemming. Pode gerar confusão por causa de radicais.
                ):
                    if _cache and _type not in self.cache:
                        with Lock():
                            self.cache[_type] = {}
                    test_word = _word.lower().replace('_', ' ')
                    if ' ' not in test_word:
                        if _cache and test_word in self.cache[_type]:
                            test_word = self.cache[_type][test_word]
                        else:
                            if _cache:
                                with Lock():
                                    if _type == 'lemmatize':
                                        self.cache[_type][test_word] = self.lemmatizer.lemmatize(test_word)
                                    elif _type == 'stemming':
                                        self.cache[_type][test_word] = self.stemmer.stem(test_word)
                                test_word = self.cache[_type][test_word]
                            else:
                                if _type == 'lemmatize':
                                    test_word = self.lemmatizer.lemmatize(test_word)
                                elif _type == 'stemming':
                                    test_word = self.stemmer.stem(test_word)
                        for item in _query:
                            if _cache and item in self.cache[_type]:
                                lemmatized_item = self.cache[_type][item]
                            else:
                                if _type == 'lemmatize':
                                    lemmatized_item = self.lemmatizer.lemmatize(item)
                                elif _type == 'stemming':
                                    lemmatized_item = self.stemmer.stem(item)
                                if _cache:
                                    with Lock():
                                        self.cache[_type][item] = lemmatized_item
                            if test_word == lemmatized_item:
                                synonyms.add(item)
                    else:
                        temp_word = []
                        for sent in self.get_pos_tag(test_word):
                            for item, pos in sent:
                                pos = self.get_wordnet_pos_tag(pos)
                                if pos is not None:
                                    temp_chave = f'{item}___{pos}'
                                    if _cache and temp_chave in self.cache[_type]:
                                        temp_word.append(self.cache[_type][temp_chave])
                                    else:
                                        if _type == 'lemmatize':
                                            temp = self.lemmatizer.lemmatize(item, pos=pos)
                                        elif _type == 'stemming':
                                            temp = self.stemmer.stem(item)
                                        temp_word.append(temp)
                                        if _cache:
                                            with Lock():
                                                self.cache[_type][temp_chave] = temp
                                else:
                                    if _cache and item in self.cache[_type]:
                                        lemmatized_item = self.cache[_type][item]
                                    else:
                                        if _type == 'lemmatize':
                                            lemmatized_item = self.lemmatizer.lemmatize(item)
                                        elif _type == 'stemming':
                                            lemmatized_item = self.stemmer.stem(item)
                                        if _cache:
                                            with Lock():
                                                self.cache[_type][item] = lemmatized_item
                                    temp_word.append(lemmatized_item)
                        if _query is not None:
                            for item_query in _query:
                                item_query = item_query.lower()  # TODO testar
                                temp_query = []
                                for sent in self.get_pos_tag(item_query):
                                    for item, pos in sent:
                                        pos = self.get_wordnet_pos_tag(pos)
                                        if pos is not None:
                                            temp_chave = f'{item}___{pos}'
                                            if _cache and temp_chave in self.cache[_type]:
                                                temp_query.append(self.cache[_type][temp_chave])
                                            else:
                                                if _type == 'lemmatize':
                                                    temp = self.lemmatizer.lemmatize(item, pos=pos)
                                                elif _type == 'stemming':
                                                    temp = self.stemmer.stem(item)
                                                temp_query.append(temp)
                                                if _cache:
                                                    with Lock():
                                                        self.cache[_type][temp_chave] = temp
                                        else:
                                            if _cache and item in self.cache[_type]:
                                                lemmatized_item = self.cache[_type][item]
                                            else:
                                                if _type == 'lemmatize':
                                                    lemmatized_item = self.lemmatizer.lemmatize(item)
                                                elif _type == 'stemming':
                                                    lemmatized_item = self.stemmer.stem(item)
                                                if _cache:
                                                    with Lock():
                                                        self.cache[_type][item] = lemmatized_item
                                            temp_query.append(lemmatized_item)
                                # TODO: verificar se deve implementar isso ordenar
                                if temp_word == temp_query:
                                    synonyms.add(item_query)
                # personal synonyms
                syn = self.synonym.get_synonym(_word)
                if syn is not None and syn not in synonyms:
                    if _query is not None:
                        if syn in _query:
                            synonyms.add(syn)
                    else:
                        synonyms.add(syn)
                if _query is not None:
                    for item in _query:
                        if item != _word and (
                                format_name(item) == format_name(_word)
                                or format_name(item.replace(' ', '')) == format_name(_word)
                        ):
                            synonyms.add(item)
                synonyms.add(_word)
                # mesh
                for syn in self.mesh.get_synonym(_word):
                    if syn is not None and syn not in synonyms:
                        if _query is not None:
                            if syn in _query:
                                synonyms.add(syn)
                        else:
                            synonyms.add(syn)
                    if _query is not None:
                        for item in _query:
                            if item != _word and (
                                    format_name(item) == format_name(_word)
                                    or format_name(item.replace(' ', '')) == format_name(_word)
                            ):
                                synonyms.add(item)
                synonyms.add(_word)
                synonyms = tuple(synonyms)
                ignore = False
                if _remove_inverted_repeated:
                    for syn in synonyms:
                        if syn in _results:
                            ignore = True
                            break
                    if ignore is False:
                        with Lock():
                            _results[_word] = synonyms
                if _word in _results:
                    with Lock():
                        _results[_word] = set(_results[_word])
                        _results[_word].remove(_word)
                        _results[_word] = tuple(_results[_word])
                if _check_reverse:
                    _temp_results = {}
                    get_synonyms_sep_1(_reverserd_word, _query, _temp_query, _remove_inverted_repeated, _temp_results,
                                       _cache, _check_reverse=False)
                    if _temp_results:  # TODO:
                        if _word in _results:
                            synonyms = set(_results[_word])
                        else:
                            synonyms = set()
                        for k in _temp_results:
                            for v in _temp_results[k]:
                                if v in _results:
                                    for s in set(_results[v]):
                                        if s != _word:
                                            synonyms.add(s)
                        for i in _temp_results:
                            for v in _temp_results[i]:
                                if v != _word:
                                    synonyms.add(v)
                        _results[_word] = tuple(synonyms)
                if _check_indirect_synonyms:  # TODO:
                    _temp_results = {}
                    for synonym in _results[_word]:
                        get_synonyms_sep_1(synonym, _query, _temp_query, _remove_inverted_repeated, _temp_results,
                                           _cache, _check_reverse=False, _check_indirect_synonyms=False)
                    if _temp_results:
                        synonyms = set(_results[_word])
                        for i in _temp_results:
                            for v in _temp_results[i]:
                                if v != _word:
                                    synonyms.add(v)
                        _results[_word] = tuple(synonyms)

        # TODO: inserir sinônimos de palavras desflexionadas
        for word in {subject} if isinstance(subject, str) else subject:
            get_synonyms_sep_1(word, query, temp_query, remove_inverted_repeated, results, cache)
        # with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        #     futures = []
        #     for word in {subject} if isinstance(subject, str) else subject:
        #         futures.append(executor.submit(get_synonyms_sep_1, word, query, temp_query, remove_inverted_repeated, results, cache))
        #     for future in as_completed(futures):
        #         if future.exception():
        #             raise future.exception()
        for k in results:
            if len(results[k]) == 0:
                results[k] = tuple([k])
        return results

    @staticmethod
    def get_wordnet_pos_tag(tag: str):
        if tag.startswith('J'):
            return wordnet.ADJ
        if tag.startswith('V'):
            return wordnet.VERB
        if tag.startswith('N'):
            return wordnet.NOUN
        if tag.startswith('R'):
            return wordnet.ADV
        else:
            return None
