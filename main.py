"""
DNS Queries Collector

The following script was designed with the purpose of posting DNS information from a customer's BIND server log to the Limu Portal.

It consists of  
"""

import requests
import json
import datetime
import pandas as pd
import argparse
import logging

# Add a CLI parser for asking the path of the Log File
parser = argparse.ArgumentParser(description='DNS Queries Collector Script')
parser.add_argument('--file_path', type=str,
                    help='Please provide the path where you have the BIND server log')

# Add a log file where the module will store the data of status process
logging.basicConfig(filename='script.log', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

# REST API Snippet 
def requestApi (data):

    # Request URL configuration for sed a DNS query
    api_domain = "https://api.lumu.io" 
    collector_id = "5ab55d08-ae72-4017-a41c-d9d735360288"
    lumu_client_key = "d39a0f19-7278-4a64-a255-b7646d1ace80"
    url = api_domain + "/collectors/" + collector_id + "/dns/queries?key=" + lumu_client_key
    
    # Body and headers: the query content
    payload = json.dumps(data)
    headers = {
        'Content-Type': 'application/json'
    }

    # Executing the request...
    response = requests.request("POST", url, headers=headers, data=payload)

    return response.status_code

# Data API Constructor
def dataLineConstructor(line):

    # Here I manipulate the content of each line
    arr = line.split(' ')
    date_format = "%d-%b-%Y"
    converted_date = datetime.datetime.strptime(arr[0], date_format)

    # Data extractor and format configuration
    time_stamp = converted_date.strftime('%Y-%m-%d') + 'T' + arr[1] + 'Z'
    name = arr[9]
    client_ip = arr[6].split('#')[0]
    client_name = arr[5].split('@')[1]
    type_of_query = arr[11]

    # Data JSON parsed
    data = {
        "timestamp": time_stamp,
        "name": name,
        "client_ip": client_ip,
        "client_name": client_name,
        "type": type_of_query
    }

    if None not in data.values():
        return data
    else:
        # Stores an error in the log if any occurs with the data parsed
        logging.info('Error at line', line)

# Setting the name module to main
if __name__ == "__main__":

    # Reading the parse arguments to catch the path file
    args = parser.parse_args()

    # Indicates when the process start
    logging.info('Script started')

    # Reading BIND Log
    with open(args.file_path, "r") as file:

        # Stores the information chunk the script will post
        chunk_data = []

        # Stores all the data parsed to be analyzed with pandas
        total_data = []

        # I tried to load line per line, but I needed to set a "Limit of lines", and send the final chunk
        file_loaded = file.readlines()

        # Limit of lines in the file
        max_lines = len(file_loaded)

        # Stores the number of lines the script has processed
        num_lines = 0

        for line in file_loaded:

            # Reads the line and parse the data
            line_data = dataLineConstructor(line)
            # Loads the data line in the chunk
            chunk_data.append(line_data)
            # Counts a line
            num_lines += 1

            if len(chunk_data) == 500 or num_lines == max_lines:
                # Posts the Chunk and stores the request status code
                information_request = requestApi(chunk_data)
                if information_request != 200:
                    # Logs if it occurs a failure with the chunk request
                    logging.info('Bad Request at chunk ', num_lines % 500, num_lines)
                # Loads the data chunk in the total data (pandas)
                total_data += chunk_data
                # Clean chunk data
                chunk_data = []

        # Crates a DataFrame with the total data Info
        df = pd.DataFrame(total_data)

        # Process the hosts statistics data using pandas
        domains = df.value_counts('name')
        domains = domains.reset_index(name='hits')
        domains['percentage'] = domains['hits'] / len(total_data) * 100
        domains['percentage'] = domains['percentage'].round(2)
        domains['percentage'] = domains['percentage'].apply(str) + '%'

        # Process the IPs statistics data using pandas
        ips = df.value_counts('client_ip')
        ips = ips.reset_index(name='hits')
        ips['percentage'] = ips['hits'] / len(total_data) * 100
        ips['percentage'] = ips['percentage'].round(2)
        ips['percentage'] = ips['percentage'].apply(str) + '%'

        # Prints statistics showing the information analyzed with pandas
        print('Total Records: ' + str(len(total_data)))
        print('')
        print('Client IPs Rank')
        print('------------------------------------------------------')
        print(ips.head(5))
        print('')
        print('Host Rank')
        print('------------------------------------------------------')
        print(domains.head(5))
        logging.info('Process Finished')