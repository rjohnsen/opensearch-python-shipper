from opensearchpy import OpenSearch, helpers
from pprint import pprint

import argparse
import os
import json
import pendulum
import re
import sys
import urllib3
import toml

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def run(): 
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("logfile")
        args = parser.parse_args()

        # Load credentials from TOML file
        with open("settings.toml", "r") as settings_file:
            settings = toml.load(settings_file)

        # Ask user for the index name
        index_name = input("Enter the index name: ")

        # Initialize the OpenSearch client
        os_client = OpenSearch(
            http_auth=(
                settings["username"],
                settings["password"]
            ),
            verify_certs=False,
            use_ssl=settings["use_ssl"],
            retry_on_timeout=True,
            timeout=60
        )

        NOW = pendulum.now()

        # Check if the file exists
        if not os.path.exists(args.logfile):
            print("Input Correct File Path")
            raise ValueError("Invalid file path provided")

        # Open the file safely
        try:
            with open(args.logfile, 'r') as log_file:
                logs = log_file.readlines()
        except Exception as e:
            print(f"Error reading file: {e}")
            sys.exit(1)

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

        # Bulk indexing
        try:
            helpers.bulk(os_client, batch_trace_logs(index_name, logs, NOW), request_timeout=60)
            print("Bulk indexing completed successfully.")
        except Exception as e:
            print(f"Error during bulk indexing: {e}")
            sys.exit(1)

    except FileNotFoundError:
        print("Settings file not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

def set_date_time(json_item_copy, NOW):
    # Adjust the timestamp logic only
    current_timestamp = pendulum.parse(json_item_copy['_source']['@timestamp'])
    
    try:
        # Safely subtract a day
        new_timestamp = current_timestamp.subtract(days=1)
    except ValueError:
        new_timestamp = current_timestamp.set(day=NOW.day, year=NOW.year, month=NOW.month)

    return str(new_timestamp)

def batch_trace_logs(index_name, logs, NOW):
    i = 0
    count = 0
    for item in logs:
        count += 1
        if i == 10000:
            print(f"Bulk Indexed {count} logs")
            i = 0

        json_item = json.loads(item)  # No need for deepcopy

        # Use the user-provided index name and update the timestamp
        new_timestamp = set_date_time(json_item, NOW)
        json_item['_source']['@timestamp'] = new_timestamp

        yield {
            "_index": index_name,  # Use provided index name directly
            "_source": json_item['_source'],
            '_op_type': "create"
        }
        i += 1

if __name__ == "__main__":
    run()
