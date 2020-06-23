"""Mirror EGA Metadata using the REST endpoints.

Starting with the list of datasets mirror all data related to that specific dataset.
"""
from typing import Generator, Iterable
import requests
import itertools
import json
import logging
from pathlib import Path
import click
import sys

FORMAT = '[%(asctime)s][%(name)s][%(process)d %(processName)s][%(levelname)-8s] (L:%(lineno)s) %(funcName)s: %(message)s'
logging.basicConfig(format=FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
logging.StreamHandler(sys.stdout)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


BASE_URL = 'https://ega-archive.org/metadata/v2/'
ENDPOINTS = ["analyses", "dacs", "runs", "samples", "studies", "files"]


def get_dataset_object(data_type: str, dataset_id: str) -> Generator:
    """Retrieve data by object type and dataset ID."""
    s = requests.Session()
    skip: int = 0
    limit: int = 10
    has_more = True
    payload = {"queryBy": "dataset",
               "queryId": dataset_id,
               "skip": str(skip),
               "limit": str(limit)}
    while has_more:
        r = s.get(f'{BASE_URL}{data_type}', params=payload)
        if r.status_code == 200:
            response = r.json()
            results_nb = int(response["response"]["numTotalResults"])
            LOG.info(f"Retrieving {limit} {data_type} for {dataset_id} from {results_nb} results.")
            for res in response["response"]["result"]:
                yield res

            if results_nb >= limit:
                limit += 10
                skip += 10
            else:
                has_more = False
        else:
            LOG.error(f"Error retrieving {data_type} for {dataset_id}. API call returned a {r.status_code}")
            has_more = False


def get_datasets(start_limit: int = 0, defined_limit: int = 10) -> Generator:
    """Retrieve datasets from EGA."""
    s = requests.Session()
    skip: int = start_limit
    limit: int = defined_limit
    has_more = True
    payload = {"skip": str(skip),
               "limit": str(limit)}
    while has_more:
        r = s.get(f'{BASE_URL}datasets', params=payload)
        if r.status_code == 200:
            response = r.json()
            results_nb = int(response["response"]["numTotalResults"])
            LOG.info(f"Retrieving {defined_limit} from {results_nb} results starting from {start_limit} results.")
            for res in response["response"]["result"]:
                yield res

            if results_nb >= limit and not defined_limit:
                limit += 10
                skip += 10
            else:
                has_more = False
        else:
            LOG.error(f"Error retrieving datasets. API call returned a {r.status_code}")
            has_more = False


def get_dataset_objects(dataset_id: str) -> Generator:
    """Retrieve information associated to dataset."""
    raw_events: Iterable = iter(())
    for endpoint in ENDPOINTS:
        LOG.info(f"Processing {endpoint} for {dataset_id} ...")
        yield itertools.chain(raw_events, get_dataset_object(endpoint, dataset_id))


def mirror_pipeline(start: int = 0, limit: int = 1) -> None:
    """Build pipeline to mirror metadata."""
    datasets = get_datasets(start_limit=start, defined_limit=limit)
    objects: Iterable = iter(())
    for dataset in datasets:
        LOG.info(f"Processing {dataset['egaStableId']} ...")
        Path(dataset["egaStableId"]).mkdir(parents=True, exist_ok=True)
        with open(f'{dataset["egaStableId"]}/dataset_{dataset["egaStableId"]}.json', 'w') as datasetfile:
            json.dump(dataset, datasetfile)
        objects = get_dataset_objects(dataset["egaStableId"])

        for idx, val in enumerate(objects):
            with open(f'{dataset["egaStableId"]}/data_{dataset["egaStableId"]}_{ENDPOINTS[idx]}.json', 'w') as datafile:
                the_data = list(val)
                json.dump(the_data, datafile)


@click.command()
@click.option('-l', '--limit-results', default=1)
@click.option('-s', '--skip-results', default=0)
def cli(limit_results: int, skip_results: int):
    """Mirror EGA dataset information.

    In order to use limit the amount of requests the limit represents the number of datasets to query per run.
    Skip parameter is used to create pipelines to resume querying datasets from a specific point the dataset list.
    """
    LOG.info(f"Start ==== >")
    mirror_pipeline(start=skip_results, limit=limit_results)
    LOG.info(f"< ==== End")

if __name__ == "__main__":
    cli()
