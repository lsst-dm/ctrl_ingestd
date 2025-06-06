# ctrl_ingestd
# Butler/Rucio ingest daemon

This daemon listens to Kafka messages from the Rucio Hermes daemons and performs butler ingests.
This can be run stand-alone or via a container.  It is configured with a YAML file, pointed to by
the enviroment variable CTRL_INGESTD_CONFIG.

The daemon listens on Kafka RSE topics that are named in the
CTRL_INGESTD_CONFIG YAML file.  For example, in the example YAML file below, the
ingestd daemon will listen on topics XRD1-test and XRD2-test.

## Example YAML file:
```
brokers: 
    - kafka:9092

group_id: "my_test_group"

num_messages: 100
timeout: 5

butler_repo: /tmp/repo

topics:
    XRD1-test:
        rucio_prefix: root://xrd1:1094//rucio
        fs_prefix: file:///rucio/disks/xrd1/rucio
    XRD2-test:
        rucio_prefix: root://xrd2:1095//rucio
        fs_prefix: file:///rucio/disks/xrd2/rucio
```

## Explanation of YAML configuration file:



REQUIRED: `brokers`
`brokers` is set to the host Kafka "kafka", listening to port 9092.

OPTIONAL: `client_id` (defaults to the hostname of the ingestd daemon)
`client_id` is the Kafka client id, which can be set to what you wish.  This should be unique if running multiple ingestd daemons on the same host.

REQUIRED: `group_id`
`group_id` is the Kafka group id, which can be set to what you wish. It should be unique if you want a single client to handle all the messages, but should be set to the same value if multiple clients are handling requests in parallel for the same repos.  In this case, the values has been set to "my_test_group".

OPTIONAL: `num_messages` (defaults to 50)
`num_messages` is the maximum message batch size of messages that are processed at one time.  In this case, it has been set to 100.

OPTIONAL: `timeout` (defaults to 1)
`timeout` is the amount of time in seconds which the Kafka consumer waits for new messages to arrive.  In this case, it has been set to 5 seconds.

REQUIRED: `butler_repo`
`butler_repo` is an indicator of the butler repository.  This can be contains Butler repository location, the path to it's `butler.yaml`, or an alias present in the file pointed to by $DAF_BUTLER_REPOSITORY_INDEX.

REQUIRED: `topics`
The `topics` section is set to the Kafka topics from which this ingestd daemon will ingest files.  The topic
name is a combination of the RSE name and the scope for that RSE.  Each topic has a mapping between the prefix of logical file names (`rucio_prefix`) and physical file names (`fs_prefix`).

In this example, The ingestd daemon listens to the topics "XRD1-test" and "XRD2-test" for messages coming from the rucio-daemons-hermesk daemon.

Note that by default, if `fs_prefix` does not exist in the YAML file, the default value will be set to empty string: ""


Changes since version 1.10:

`brokers` section is now a list

The `butler` section and `repo` subsection has been replaced by `butler_repo`.

`fs_prefix` is set to empty string "" by default, if it does not exist in the YAML file.

rucio_prefix and fs_prefix will now automatically append a "/" to the end of the string name if it does not exist.
The only exception to this is if `fs_prefix` is set to empty string: ""
