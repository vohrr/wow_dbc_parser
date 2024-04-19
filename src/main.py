import requests
import csv
import json
import os

BETA_REPO = 'https://raw.githubusercontent.com/Vladinator/wow-dbc-archive/release/wow_beta_latest'
LIVE_REPO = 'https://raw.githubusercontent.com/Vladinator/wow-dbc-archive/release/wow_latest'
PTR_REPO = 'https://raw.githubusercontent.com/Vladinator/wow-dbc-archive/release/wow_ptr_latest'


def get_file_from_repo(version:str, file_name:str):
    requestUrl = f"{version}/{file_name}"
    
    print(f"Fetching data from {file_name}...")
    
    with open(f"./json/{file_name.replace(".csv","")}.json", "w") as jsonfile:
       
        with (requests.get(requestUrl, stream=True)) as response:
       
            if response.ok:
                lines = (line.decode('utf-8') for line in response.iter_lines())
                data = []
                for row in csv.DictReader(lines):
                    data.append(row)
                json.dump(data, jsonfile)
       
                print(f"Done. response code: {response.status_code}, {response.elapsed} elapsed.")
            else:
                jsonfile.close()
                os.remove(f"./json/{file_name.replace(".csv","")}.json")
                
                print(f"Error retrieving data from {file_name}. Response: {response.status_code} {response.reason}")
            
def main():
    get_file_from_repo(LIVE_REPO, "talenttree.csv")





main()