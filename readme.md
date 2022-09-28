# Description
This tool will allow you to migrate data between Cassandra Clusters.
There are 3 scripts that will handle this task. Each script have its own purpose and usage, which i'll describe in detail below.


The `create-snapshot.sh` script will be sent to the ***Source*** Cassandra nodes and execute `nodetool cleanup`, `nodetool snapshot`, compress the snapshots and then send them to an S3 bucket.
To be able to upload to an S3 bucket, you'll need `gimme-aws-creds` installed on your local machine.
The `<snapshot>.tar.gz` file will be removed to save space after the snapshot has been uploaded to S3.

The `restore-snapshot.sh` script will be sent to the ***Target*** Cassandra nodes and use `cqlsh` to create the Keyspaces from the Source snapshot, this will also create the Tables that we need in order to start pushing the raw `.db` files using `SSTableLoader`.

All Cassandra operations are executed from [cassutils](./cassutils/Dockerfile) docker image.


`start.sh` is the one (and only) script you'll execute in your local-machine. Usage described below.

# Usage
Complete the following to trigger a migration:
 - Modify the file [config-sample.toml](config-sample.toml) to suit your needs and save it as `config.toml`
 - Login to AWS CLI, set the `AWS_PROFILE` env var and validate account number with `aws sts get-caller-identity | jq -r .Account`
 - Launch the main script specifying your config file `./start.sh migrate --config config.toml`

More options:
```text
>>> Actions:
snapshot       - Create snapshot (only source cluster will be used)
restore        - Restore from snapshot (only target cluster will be used)
migrate        - Create snapshot in source cluster and restore in target cluster
validate       - Validate keyspace and table data in source or target cluster
                 (Usage: validate source | validate target)
>>> Options:
-c|--config    - Specify a config file to use (Optional, default: config.toml)
-n|--node-id   - Specify a node to run the desired action instead of running in all nodes (Optional)
--wait         - Wait for each node to finish before continuing with the next.
                 Disables working with all nodes at the same time. (Optional. default: False)
--no-upload    - Skip uploading the snapshot to S3. (Optional. Default: false)
                 This will just take a snapshot and save it to <work-path>/snapshot/
--no-download  - Skip downloading the snapshot from S3. (Optional. Default: false)
                 This asumes the snapshot is already present in <work-path>/snapshot/<snapshot-name> and will restore from there
-h|--help      - Show this help message.
```
```bash
>>> Usage (Single task):
./start.sh snapshot --config custom-migration-config.toml
./start.sh restore --no-download
>>> Usage (Full migration):
./start.sh migrate
>>> Usage (Run data validation in target cluster):
./start.sh validate target --config custom-migration-config.toml --node-id 0
```
