import functools
import os

import pandas as pd
import requests

BASE_URL = 'http://localhost:9090/api/v1'
START = '2022-06-26T00:00:00Z'
END = '2022-06-26T00:01:00Z'
OUTPUT_PATH = 'results'
STEP = '15s'


def read_lines(path: str):
    with open(path, 'r') as file:
        lines = file.readlines()
    lines = map(str.strip, lines)
    lines = filter(lambda s: len(s) > 0, lines)
    lines = list(lines)
    return lines


def read_queries():
    return read_lines('config/queries.txt')


def execute_queries():
    results = dict()
    for query in read_queries():
        response = requests.get(
            f'{BASE_URL}/query_range',
            params=dict(
                query=query,
                start=START,
                end=END,
                step=STEP,
            ),
            verify=False)
        # matrix
        matrix = response.json()['data']['result']
        for vector in matrix:
            instance_name = vector['metric']['instance']
            values = vector['values']
            instance = results.get(instance_name, [])
            df = pd.DataFrame(values, columns=['timestamp', query])
            instance.append(df)
            results[instance_name] = instance
    for i, instance_name in enumerate(sorted(results)):
        instance = results[instance_name]
        df = functools.reduce(lambda a, b: a.merge(b, on='timestamp'), instance)
        df['instance_name'] = instance_name
        df.to_csv(f'{OUTPUT_PATH}/{i}.csv')


def main():
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    execute_queries()


if __name__ == '__main__':
    main()
