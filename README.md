# `ingestd`

This daemon listens to Kafka messages of type `transfer-done` emitted by the [Rucio Hermes](https://github.com/lsst-dm/ctrl_rucio_ingest)
daemon and ingests those files into the configured local Butler repos.

The daemon can be run stand-alone or via a container. It is configured with a YAML file provided
as a command line argument or via the environmental variable `CTRL_INGESTD_CONFIG`.

## Usage

The usage of this daemon is shown below:

```
usage: ingestd [-h] [-c FILE] [-D]

Listens to Kafka messages on successfully replicated files and ingest them into a local Butler.

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
kafka_num_messages: 10
kafka_client_timeout: 3.0
rucio_scope: butler_datastore_dir
butler: https://host.example.org:1234/path/to/rse/butler_datastore_dir/butler.yaml
```

where:

* `kafka_brokers` _(required)_ is a list of at least one Kafka broker `ingestd` must connect to to retrieve messages,
* `kafka_topic` _(required)_ is the topic of the Kafka messages `ingestd` must listen to. This topic is typically the name of the destination Rucio Storage Element where the files were correctly replicated,
* `kafka_num_messages` _(optional)_ is the maximum number of Kafka messages to retrieve in one batch. Default value: 50,
* `kafka_client_timeout` _(optional)_ is the timeout in seconds (`float`) `ingestd` waits for retrieving one batch of Kafka messages. Default value: 5.0 seconds,
* `rucio_scope` _(required)_ is the Rucio scope of the replicated files this instance of `ingestd` must consider for ingestion into the local Butler repo. As per [DMTN-213](https://dmtn-213.lsst.io) the Rucio scope is the basename of the root directory of the Butler repo datastore. `ingestd` will ignore messages involving replicated files with other Rucio scopes.
* `butler` _(required)_ is the configuration of the local Butler repository `ingestd` must ingest replicated files into. This Butler repo must exist. This configuration file can be expressed in one of the forms below:

  * the absolute URL of the Butler configuration file, e.g. `https://host.example.org:1234/path/to/rse/butler_datastore_dir/butler.yaml`,
  * the absolute URL of the directory where the Butler configuration file `butler.yaml` can be found, e.g. `hhttps://host.example.org:1234/path/to/rse/butler_datastore_dir/`,
  * an alias to a Butler configuration present in the file pointed to by the environment variable `DAF_BUTLER_REPOSITORY_INDEX`.

## How to run

To run stand-alone do:

```bash
# Set up a release of the LSST Science Pipelines
source /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/w_2024_45/loadLSST.sh
setup lsst_distrib

# Clone this repo and run scons
git clone https://github.com/lsst-dm/ctrl_ingestd.git
cd ctrl_ingestd
export PYTHONPATH=${PWD}/python:${PYTHONPATH}
scons

# Run ingestd
bin/ingestd --help
```
