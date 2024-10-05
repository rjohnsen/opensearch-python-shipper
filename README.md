## OpenSearch Python Shipper

### About

Demo script for showing how you can utilize Python and OpenSearch Python API to ingest logs. 
This demo script will ingest Ndjson log files. 

## Installation

Run PIP install before running the shipper for the first time to install requirements: 

```bash
pip install -r requirements.txt
```

## Before use

Before running this shipper, place a file called **settings.toml** in current folder. Add and adjust the following settings to your need: 

```toml
hostname = "https://127.0.0.1:9200"
username = "admin"
password = ""
use_ssl = true
```

## Usage

```bash
python3 shipper.py path_to_log_file
```

Example:

```bash
python3 shipper.py ./log.ndjson
```

### Notes

The scriptt will ask for which index to ingest the log into. If the ingestion fails, you should remove indices manually in OpenSearch.

