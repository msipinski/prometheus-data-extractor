import functools

import pandas as pd
import requests

BASE_URL = 'http://localhost:9090/api/v1'
START = '2022-06-26T00:00:00Z'
END = '2022-06-26T00:01:00Z'
OUTPUT_PATH = 'result.csv'
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
    frames = []
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
        data = response.json()['data']['result']
        # TODO: for multiple jobs/instances
        data = data[0]
        # values for first job/instance
        data = data['values']
        df = pd.DataFrame(data, columns=['timestamp', query]).set_index('timestamp')
        frames.append(df)
    df = functools.reduce(lambda a, b: a.join(b), frames)
    df.to_csv(OUTPUT_PATH)


def main():
    execute_queries()


if __name__ == '__main__':
    main()
