# `ingestd`

This daemon listens to Kafka messages of type `transfer-done` emitted by the [Rucio Hermes](https://github.com/lsst-dm/ctrl_rucio_ingest)
daemon and ingests those files into the configured local Butler repos.

The daemon can be run stand-alone or via a container. It is configured with a YAML file provided
as a command line argument or via the environmental variable `CTRL_INGESTD_CONFIG`.

## Usage

The usage of this daemon is shown below:

```
usage: ingestd [-h] [-c FILE] [-D]

Listens to Kafka events on successfully replicated files and ingest them into a local Butler.

options:
  -h, --help            show this help message and exit
  -c FILE, --config FILE
                        Configuration file for this daemon in YAML format. Default: value of the environment variable CTRL_INGESTD_CONFIG.
  -D, --debug           Set log level to DEBUG. Default: INFO.
```

## Configuration file

This is an example of a valid configuration file in YAML format:

```yaml
kafka_brokers:
- broker1.example.org:1234
- broker2.example.org:1234
- broker3.example.org:1234
kafka_topic: DF_BUTLER_DISK
kafka_num_messages: 50
kafka_client_timeout: 1.0
butlers:
  repo_1: https://host.example.org:1234/path/to/rse/repo_1/butler.yaml
  repo_2: https://host.example.org:1234/path/to/rse/repo_2/butler.yaml
  repo_3: https://host.example.org:1234/path/to/rse/repo_3/butler.yaml
```

* `kafka_brokers`: _(required)_ list of at least one Kafka broker `ingestd` must connect to to retrieve messages,
* `kafka_topic`: _(required)_ Kafka topic `ingestd` must listen to. The topic is typically the name of the destination Rucio Storage Element where the files were correctly replicated,
* `kafka_num_messages`: _(optional)_ maximum number of Kafka messages to retrieve in one batch. Default value: 50,
* `kafka_client_timeout`: _(optional)_ timeout in seconds (`float`) `ingestd` waits for retrieving one batch of Kafka messages. Default value: 5.0,
* `butlers`: _(required)_ map of at least one Rucio scope to its corresponding Butler configuration file. As per [DMTN-213](https://dmtn-213.lsst.io) the Rucio scope is the
basename of the root directory of the Butler repo datastore. Kafka messages include the Rucio scope of the replicated file. Since `ingestd` uses that scope
to determine which Butler repo it must ingest the file into, the key of the map must be identical to the Rucio scope for each repo.

`ingestd` expects that all the configured Butler repos already exist. A Butler configuration file can be one of:

* the absolute URL of the Butler configuration file, e.g. `https://host.example.org:1234/path/to/rse/repo_1/butler.yaml`,
* the absolute URL of the directory where the Butler configuration file `butler.yaml` can be found, e.g. `https://host.example.org:1234/path/to/rse/repo_1/`,
* an alias to a Butler configuration present in the file pointed to by the environment variable `DAF_BUTLER_REPOSITORY_INDEX`.

## How to run

To run stand-alone do:

```bash
# Set up a release of the LSST Science Pipelines
source /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/w_2024_40/loadLSST.sh
setup lsst_distrib

# Clone this repo and run scons
git clone https://github.com/lsst-dm/ctrl_ingestd.git
cd ctrl_ingestd
export PYTHONPATH=${PWD}/python:${PYTHONPATH}
scons

# Run ingestd
bin/ingestd --help
```
