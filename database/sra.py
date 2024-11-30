#!/usr/bin/env python3

if __name__ == "__main__":
    import getpass
    from data.database import Database
    from data.sra import SRA
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('--database', required=True)
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--password', action='store_true')
    parser.add_argument('--port', default=None)
    parser.add_argument('--query', required=True)
    parser.add_argument('--query_public', action='store_true', default=True)
    parser.add_argument('--user', default=None)
    arguments = parser.parse_args()

    password = getpass.getpass('Database password: ') if arguments.password else None
    database = Database(
        database=arguments.database,
        host=arguments.host,
        user=arguments.user,
        password=password,
        port=arguments.port
    )
    SRA().search(arguments.query, only_public=arguments.query_public)
    database.create_from_xml('database.xml', create_structure=True)
    print('Finished.', flush=True)

