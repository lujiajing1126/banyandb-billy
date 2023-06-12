# Billy Benchmark for BanyanDB

## How to run

First, start `banyandb`. And create resources,

```shell
$ bydbctl group create -f ./scripts/group.yaml
$ bydbctl measure create -f ./scripts/measure.yaml
```

After schema are setup, 

```shell
$ ./scripts/load_data_5K_client.sh
```

which will load 5K measure per minute (24 hours in total) into the BanyanDB.

## Acknowledgement

- [How ScyllaDB Scaled to One Billion Rows a Second](https://www.scylladb.com/2019/12/12/how-scylla-scaled-to-one-billion-rows-a-second/)
- https://github.com/VictoriaMetrics/billy
