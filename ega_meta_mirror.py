"""Mirror EGA Metadata using the REST endpoints.

Starting with the list of datasets mirror all data related to that specific dataset.
"""
from typing import Dict, Generator, Iterable
import requests
import itertools
import json
import logging
from pathlib import Path
import click

FORMAT = '[%(asctime)s][%(name)s][%(process)d %(processName)s][%(levelname)-8s] (L:%(lineno)s) %(funcName)s: %(message)s'
logging.basicConfig(format=FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
LOG = logging.getLogger(__name__)

BASE_URL = 'https://ega-archive.org/metadata/v2/'
ENDPOINTS = ["analyses", "dacs", "runs", "samples", "studies", "files"]


def process_result(response: Dict) -> Generator:
    """Process response metadata and get all results."""
    for i in response["response"]["result"]:
        yield i


def retrieve_data(data_type: str, dataset_id: str) -> Generator:
    """Retrieve data by object type and dataset ID."""
    skip: int = 0
    limit: int = 10
    has_more = True
    payload = {"queryBy": "dataset",
               "queryId": dataset_id,
               "skip": str(skip),
               "limit": str(limit)}
    while has_more:
        r = requests.get(f'{BASE_URL}{data_type}', params=payload)
        if r.status_code == 200:
            response = r.json()
            results_nb = int(response["response"]["numTotalResults"])
            for res in process_result(response):
                yield res

            if results_nb >= limit:
                limit += 10
            else:
                has_more = False
        else:
            has_more = False


def process_datasets(start_limit: int = 0, defined_limit: int = 10) -> Generator:
    """Retrieve datasets from EGA."""
    skip: int = start_limit
    limit: int = defined_limit
    has_more = True
    payload = {"queryBy": "dataset",
               "skip": str(skip),
               "limit": str(limit)}
    while has_more:
        r = requests.get(f'{BASE_URL}datasets', params=payload)
        if r.status_code == 200:
            response = r.json()
            results_nb = int(response["response"]["numTotalResults"])
            for res in process_result(response):
                yield res

            if results_nb >= limit and not defined_limit:
                limit += 10
            else:
                has_more = False
        else:
            has_more = False


def retrieve_dataset_info(dataset_id: str) -> Generator:
    """Retrieve information associated to dataset."""
    raw_events: Iterable = iter(())
    for endpoint in ENDPOINTS:
        yield itertools.chain(raw_events, retrieve_data(endpoint, dataset_id))


def main(start: int = 0, limit: int = 1) -> None:
    """Build pipeline to mirror metadata."""
    datasets = process_datasets(start_limit=start, defined_limit=limit)
    results: Iterable = iter(())
    for dataset in datasets:
        Path(dataset["egaStableId"]).mkdir(parents=True, exist_ok=True)
        with open(f'{dataset["egaStableId"]}/dataset_{dataset["egaStableId"]}.json', 'w') as datasetfile:
            json.dump(dataset, datasetfile)
        results = retrieve_dataset_info(dataset["egaStableId"])

        for idx, val in enumerate(results):
            with open(f'{dataset["egaStableId"]}/data_{dataset["egaStableId"]}_{ENDPOINTS[idx]}.json', 'w') as datafile:
                the_data = list(val)
                json.dump(the_data, datafile)


@click.command()
@click.option('-limit', '--limit-results', default=1)
@click.option('-skip', '--skip-results', default=0)
def cli(limit_results: int, skip_results: int):
    """Mirror EGA dataset information.

    In order to use limit the amount of requests the limit represents the number of datasets to query per run.
    Skip parameter is used to create pipelines to resume querying datasets from a specific point the dataset list.
    """
    main(start=skip_results, limit=limit_results)


if __name__ == "__main__":
    cli()
