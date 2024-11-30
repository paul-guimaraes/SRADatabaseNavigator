from requests.utils import requote_uri
from urllib.request import urlopen
from lxml.etree import parse
from lxml.etree import XMLParser
import progressbar
from time import sleep
from time import time


class SRA:
    def __init__(self):
        # Entrez Utilities request URL.
        self.__url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils'
        self.__database = 'sra'
        self.__max_request_by_second = 3
        self.__last_request_time = []
        # items by package.
        self.__pagination = 400

    def is_max_request_alcanced(self):
        self.__last_request_time.insert(0, time())
        if len(self.__last_request_time) <= self.__max_request_by_second:
            return False
        else:
            oldest = self.__last_request_time.pop()
            if self.__last_request_time[0] - oldest < 3:
                return True
        return False

    def fetch(self, database_name, ids):
        query = f'{self.__url}/efetch.fcgi' \
                f'?db={database_name}' \
                f'&retmode=xml' \
                f'&id={ids if isinstance(ids, str) else ",".join(ids)}'
        return self.query(query)

    def query(self, query):
        tentativa = 10
        while tentativa > 0:
            try:
                if self.is_max_request_alcanced():
                    sleep(2)
                document = urlopen(query)
                while document.status != 200:
                    sleep_time = 1
                    print(f'Retrieving data package failed. {document.status} error. Retrying in {sleep_time} seconds...', flush=True)
                    sleep(sleep_time)
                data = parse(document, XMLParser(remove_blank_text=True, remove_comments=True))
                document.close()
                return data
            except Exception as err:
                tentativa -= 1
                print(err)
        return None

    def search(self, term, only_public=True, database_file='database.xml'):
        experiments = None
        publications = {}

        query = f'{self.__url}/esearch.fcgi?db={self.__database}&term={term}'
        if only_public:
            query += f'+AND+"public"[Access]'
        query = requote_uri(query)
        query += f'&RetMax={self.__pagination}'

        continuar = True
        status_bar = progressbar.ProgressBar()
        step = 0
        records_count = 0

        while True:
            records = self.query(f'{query}&RetStart={step * self.__pagination}')
            ids = [item.text for item in records.findall('./IdList/Id')]

            if step == 0:
                # getting status information
                records_count = int(records.findtext('./Count'))
                print(f'{records_count} records founded.', flush=True)
                status_bar = progressbar.ProgressBar(
                    maxval=int(records_count / self.__pagination) + 1,  # for multiple
                    # maxval=int(records_count),
                    widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()],
                )
                print(f'Retrieving data...', flush=True)
                status_bar.start()
            if (step * self.__pagination) >= records_count:
                break
            # Retrieving data

            # getting experiment data
            experiment = self.fetch(self.__database, ','.join(ids))

            # Querying related literacture from pubmed
            links_query = f'{self.__url}/elink.fcgi?dbfrom={self.__database}&db=pubmed&id={",".join(ids)}'
            publication = self.query(links_query)
            linked_ids = {}
            for item in publication.findall('.LinkSet/LinkSetDb'):
                if item.find('DbTo') is not None and item.find('DbTo').text == 'pubmed':
                    if item.find('DbTo').text not in linked_ids:
                        linked_ids[item.find('DbTo').text] = []
                    for link in item.findall('Link/Id'):
                        linked_ids[item.find('DbTo').text].append(link.text)
            for ncbi_database in linked_ids:
                if len(linked_ids[ncbi_database]) > 0:
                    chaves = ",".join(linked_ids[ncbi_database])
                    if chaves in publications:
                        publication = publications[chaves]
                    else:
                        publication = self.fetch(ncbi_database, chaves)
                        publications[chaves] = publication
                    for item in publication.findall('./'):
                        pmid = item.find('MedlineCitation/PMID')
                        if pmid is not None and pmid.text is not None:
                            for child in experiment.xpath(f'//XREF_LINK[DB="pubmed" and ID="{pmid.text}"]'):
                                parent = child.xpath('ancestor::EXPERIMENT_PACKAGE')[0]
                                if item not in parent:
                                    parent.append(item)
            if experiments is None:
                experiments = experiment
            else:
                for item in experiment.findall('./'):
                    experiments.getroot().append(item)
            step += 1
            status_bar.update(step)
        status_bar.finish()
        experiments.write(database_file)
