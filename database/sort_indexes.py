#!/usr/bin/env python3

import argparse
import pandas as pd

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--index_file', required=True)

    args = parser.parse_args()

    dataframe = pd.read_csv(args.index_file)
    dataframe = dataframe.sort_values(by=['Connections', 'Attributes', 'Values'], ascending=False)
    dataframe.to_csv(f'{args.index_file.rsplit(".", maxsplit=1)[0]}_sorted.csv', index=False)
