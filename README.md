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

  Mirror EGA dataset information.

  In order to use limit the amount of requests the limit represents the
  number of datasets to query per run. Skip parameter is used to create
  pipelines to resume querying datasets from a specific point the dataset
  list.

Options:
  -limit, --limit-results INTEGER
  -skip, --skip-results INTEGER
  --help                          Show this message and exit.

```

The script can be run using:

* `python ega_meta_mirror.py -limit 1 -skip 0`
* `python ega_meta_mirror.py -limit 2`
