## Mirror Metadata from EGA 

Make use of https://ega-archive.org/metadata/how-to-use-the-api in order to mirror dataset metadata

**Requires Python 3.6+**

### Usage

Installation is done with the following instructions:
```
git clone
pip install -r requirements.txt
```

Check if installed properly:
```
➜ python ega_meta_mirror.py --help
Usage: ega_meta_mirror.py [OPTIONS]

  Mirror EGA dataset metadata information.

  In order to use limit the amount of requests the limit represents the
  number of datasets to query per run. Skip parameter is used to create
  pipelines to resume querying datasets from a specific point the dataset
  list. Using the -d option allows to specify the dataset to mirror.

Options:
  -l, --limit-results INTEGER  Number of results.
  -s, --skip-results INTEGER   Skip the first n results.
  -d, --dataset TEXT           Download a specific dataset, will ignore limit
                               and skip options.

  --help                       Show this message and exit.
```

The script can be run using:

* `python ega_meta_mirror.py -l 1 -s 0` for mirroring first dataset;
* `python ega_meta_mirror.py -l 2 -s 3` for mirroring 2 datasets after the first 3;
* `python ega_meta_mirror.py -l 4 ` - for mirroring first 4 datasets;
* `python ega_meta_mirror.py -d EGAD00001002894` - for mirroring dataset `EGAD00001002894`.


#### Using the Metadata 

In order use the metadata into a Standalone Sensitive Data Archive the data can be imported into an existing MongoDB or a new one.
