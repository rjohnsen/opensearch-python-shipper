from opensearchpy import OpenSearch, helpers
import os
import json
import pendulum
import re
import sys
import copy
import urllib3
import termcolor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# User settings
hostname = ""  # CHANGE THIS!
username = ""  # CHANGE THIS!
password = ""  # CHANGE THIS!

# Initialize the OpenSearch client with TLS verification turned off
os_client = OpenSearch(
    http_auth=(username, password),
    verify_certs=False,  # Disable TLS/SSL certificate verification
    use_ssl=True,
    retry_on_timeout=True,
    timeout=60
)

NOW = pendulum.now()

# Check if the log file path is provided via command-line arguments
if len(sys.argv) < 2:
    print("Please provide a log file path as an argument.")
    sys.exit(1)

file_path = sys.argv[1]
print(f"Using logfile: {file_path}")

# Check if the file exists
if not os.path.exists(file_path):
    print("Input Correct File Path")
    raise ValueError("Invalid file path provided")

# Open the file safely
try:
    with open(file_path, 'r') as log_file:
        logs = log_file.readlines()
except Exception as e:
    print(f"Error reading file: {e}")
    sys.exit(1)

# Ask user for the index name
index_name = input("Enter the index name: ")

# Check if the index exists, if not create it with the desired field limit
if not os_client.indices.exists(index=index_name):
    print(f"Index '{index_name}' does not exist. Creating it now.")
    try:
        os_client.indices.create(
            index=index_name,
            body={
                "settings": {
                    "index.mapping.total_fields.limit": 2000  # Set desired field limit here
                }
            }
        )
        print(f"Index '{index_name}' created successfully with field limit 2000.")
    except Exception as e:
        print(f"Error creating index: {e}")
        sys.exit(1)
else:
    print(f"Index '{index_name}' already exists. Updating field limit to 2000.")
    try:
        os_client.indices.put_settings(
            index=index_name,
            body={
                "index.mapping.total_fields.limit": 2000  # Adjust this number as needed
            }
        )
        print(f"Field limit for index {index_name} updated successfully.")
    except Exception as e:
        print(f"Error updating field limit: {e}")
        sys.exit(1)

def set_date_time(json_item_copy):
    # Adjust the timestamp logic only
    current_timestamp = pendulum.parse(json_item_copy['_source']['@timestamp'])
    try:
        # Safely subtract a day
        new_timestamp = current_timestamp.subtract(days=1)
    except ValueError:
        new_timestamp = current_timestamp.set(day=NOW.day, year=NOW.year, month=NOW.month)

    return str(new_timestamp)

def batch_trace_logs(logs):
    i = 0
    count = 0
    for item in logs:
        count += 1
        if i == 10000:
            print(f"Bulk Indexed {count} logs")
            i = 0

        json_item = copy.deepcopy(item)
        json_item = json.loads(json_item)

        # Use the user-provided index name and update the timestamp
        new_timestamp = set_date_time(json_item)
        json_item['_source']['@timestamp'] = new_timestamp

        yield {
            "_index": index_name,  # Use provided index name directly
            "_source": json_item['_source'],
            '_op_type': "create"
        }
        i += 1

# Delete Data Streams
print("Deleting Existing Indices")
try:
    os_client.indices.delete_data_stream(name='*')
    print("Data streams deleted successfully.")
except Exception as e:
    print(f"Error deleting data streams: {e}")
    sys.exit(1)

# Bulk indexing
try:
    helpers.bulk(os_client, batch_trace_logs(logs), request_timeout=60)
    print("Bulk indexing completed successfully.")
except Exception as e:
    print(f"Error during bulk indexing: {e}")
    sys.exit(1)