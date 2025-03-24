# ctrl_ingestd
# Butler/Rucio ingest daemon

This daemon listens to Kafka messages from the Rucio Hermes daemons and performs butler ingests.
This can be run stand-alone or via a container.  It is configured with a YAML file, pointed to by
the enviroment variable CTRL_INGESTD_CONFIG.

The daemon listens on Kafka RSE topics that are named in the
CTRL_INGESTD_CONFIG YAML file.  For example, in the example YAML file below, the
ingestd daemon will listen on topics XRD1 and XRD2.

## Example YAML file:
```
brokers: kafka:9092

group_id: "my_test_group"

num_messages: 50

butler:
    repo: /tmp/repo

topics:
    XRD1-test:
        rucio_prefix: root://xrd1:1094//rucio
        fs_prefix: file:///rucio/disks/xrd1/rucio
    XRD2-test:
        rucio_prefix: root://xrd2:1095//rucio
        fs_prefix: file:///rucio/disks/xrd2/rucio
```

## Explanation of configuration file:

"brokers" is set to the host Kafka "kafka", listening to port 9092.

"group_id" is the Kafka group id, which can be set to what you wish. It should be unique if you want a single client to handle all the messages, but should be set to the same value if multiple clients are handling requests in parallel for the same repos.  In this case, the values has been set to "my_test_group".

"num_messages" is the maximum message batch size of messages that are processed at one time.  In this case, it has been set to 50.

"butler" section is set to the butler configuration.  It contains the location of the Butler repository.

The "topics" section is set to the Kafka topics from which this ingestd daemon will ingest files.  The topic
name is a combination of the RSE name and the scope for that RSE.  Each has a prefix mapping between logical file names and physical file names.

The ingestd daemon listens to the topics XRD1 and XRD2 for messages coming from the rucio-daemons-hermesk daemon.
