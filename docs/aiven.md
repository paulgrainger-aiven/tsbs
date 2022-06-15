# TSBS Guide: Benchmarking on Aiven for ClickHouse

This guide explains one way to benchmark ingest performance on Aiven for ClickHouse, using the IoT sample dataset. 

This will assume the reader has access to both the Aiven Console, as well as a cloud account of their choosing. 

In addition, this assumes the reader is familiar with provisioning VMs in the cloud of their choosing, as well as knowing how to access the machine via ssh. 

## Setup

First, determine where you would like this benchmark to run (cloud provider and region). You should use this same combination for both your VM and Aiven for ClickHouse service in the next steps. 

Once you have decided on that, provision a VM in that cloud and region. The steps provided next assume a Debian based VM. The recommended configuration is 16 CPU and at least 100 GB of storage. 

Once the machine is available, ssh onto it and install the necessary software

```
# base packages
sudo apt update
sudo apt install -y python3-pip wget git make
pip install aiven-client

# install go
GO_VERSION=go1.18.3
wget https://go.dev/dl/${GO_VERSION}.linux-amd64.tar.gz
tar xvf ${GO_VERSION}.linux-amd64.tar.gz 
sudo chown -R root:root ./go
sudo mv go /usr/local
export GOPATH=$HOME/work
export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

# pull repo and build go libraries and binaries
git clone https://github.com/paulgrainger-aiven/tsbs.git
cd tsbs
make
export PATH=$PATH:$PWD/cmd:$PWD/bin
```
## Generating benchmark data

There is a wrapper script available which will generate data to benchmark ClickHouse ingestion rates. The script is available [here.](../scripts/aiven/generate-clickhouse-iot.sh)

Usage is as follows:

```
cd scripts
chmod u+x aiven/*
./aiven/generate-clickhouse-iot.sh
```

By default this will create 3 months of simulated data. To change this, adjust the timestamps within the file. The script will process the data generation in parallel. Try not to exceed num_cpu / 2 as each job requires 2 threads, one to generate data, another to gzip it. 

This may take upwards of an hour to complete, longer if additional date ranges are added. 

## Running the benchmark against Aiven

To execute the benchmark, first you must log in via the Aiven client. It is recommended that you follow the steps to create an API token for this. 

[Create an API Token](https://developer.aiven.io/docs/platform/howto/create_authentication_token)

[Authenticate on CLI with token](https://developer.aiven.io/docs/tools/cli.html#authenticate)

Ensure after you login, that you are in the correct project

```
avn project switch my-project
```

You can now execute the Aiven load (provided the data generation has completed)

```
BATCH=50000 \
  NAME=my-clickhouse \
  CLOUD=google-us-west1 \ 
  PLAN=startup-beta-16 \
  ./aiven/load-aiven.sh
```

Where

- BATCH: Batch size to be loaded into ClickHouse on each iteration. 
- NAME: The Aiven service name
- CLOUD: The cloud this will run on (ensure its the same as the VM)
- PLAN: The Aiven plan

This script will provision an Aiven for ClickHouse service in your project, and once its available begin the ingest benchmark. 

