import csv
from datetime import datetime

from progressbar import ProgressBar, Bar, Percentage
from re import sub
import numpy as np

NONE_VALUES = {
    '',
    '-',
    '--',
    'missing',
    '#n/a',
    'n/a',
    'na',
    'none',
    'not applicable',
    'not apply',
    'not available',
    'not collected',
    'not described',
    'not provided',
    'not recorded',
    'unknown',
    'unkown',
}


class StatusBar:
    def __init__(self, task_name, size):
        self.__task_name = task_name
        self.__size = size

    def __enter__(self):
        self.__status_bar = ProgressBar(
            maxval=self.__size,
            widgets=[f'{self.__task_name}', ' ', Percentage(), ' ', Bar(), ' '],
        )
        self.__status_bar.start()
        self.__start_time = datetime.now()
        return self

    def __exit__(self, *args):
        self.__status_bar.finish()
        print(f'{self.__task_name} finished in {datetime.now() - self.__start_time}.')

    def update(self):
        if self.__status_bar.value + 1 < self.__size:
            self.__status_bar.update(self.__status_bar.value + 1, force=True)


def format_name(name):
    name = name.lower()
    name = sub('\[.*?\]', '', name)
    name = name.replace('/', '_')
    while '  ' in name:
        name = name.replace('  ', ' ')
    name = sub('\W', '_', name)
    while '__' in name:
        name = name.replace('__', '_')
    while name.endswith('_'):
        name = name[:-1]
    return name


def get_combinations(data_keys: list | tuple):
    # Alternativa à itertools.combination 2 a 2.
    data_index = np.arange(len(data_keys))
    idx = np.stack(np.triu_indices(len(data_index), k=1), axis=-1)

    for a, b in data_index[idx]:
        yield data_keys[a], data_keys[b]

    del idx


def dict_to_csv(data: dict, file):
    columns = [column for column in sorted(data)]
    csv_writer = csv.writer(file, delimiter=',', quotechar='"')
    if len(columns) > 0:
        csv_writer.writerow(columns)
        for j, _ in enumerate(data[columns[0]]):
            values = []
            for column in columns:
                values.append(data[column][j])
            csv_writer.writerow(values)
