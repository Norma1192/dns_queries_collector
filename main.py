"""
DNS Queries Collector

The following script was designed with the purpose of posting DNS information from a customer's BIND server log to the Limu Portal.

It consists of  
"""

import requests
import json

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