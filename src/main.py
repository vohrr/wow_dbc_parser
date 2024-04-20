import requests
import csv
import json
import os
import pandas

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
                    for value in row.items():
                        if value[1]:
                            try:
                                if int(value[1]) != None:
                                    row[value[0]] = int(value[1])
                                elif bool(value) != None:
                                    row[value[0]] = bool(value[1])
                            except Exception:
                                continue   
                    
                    data.append(row)
                
                json.dump(data, jsonfile, separators=(',',':'))
        
                print(f"Done. response code: {response.status_code}, {response.elapsed} elapsed.")
            else:
                jsonfile.close()
                os.remove(f"./json/{file_name.replace(".csv","")}.json")
                
                print(f"Error retrieving data from {file_name}. Response: {response.status_code} {response.reason}")


def get_data_from_json(file_name:str):    
    file_path = get_file_name(file_name)
    try:
        file_data = json.load(open(file_path)) 
        for item in range(0,10):
            print(file_data[item])
    except FileNotFoundError as exception:
        print(f"Error: {file_name} not found. {exception}")

def merge_spell_data(base_name:str,merge_name:str):
    base_file = get_file_name(base_name)
    merge_file = get_file_name(merge_name)
    merged = {}
    print(f"Reading {base_name}...")
    
    
    print(f"Done. Reading {merge_file}...")
    
      
    print("Done. Merging...")
    
    
    print("Done")
    
    spell_json_file = f"./merged_json/spell_data.json"
    
    json.dump(merged, spell_json_file)

def get_file_name(name:str)->str:
    return f"./json/{name}.json" 

def main():
    #get_file_from_repo(LIVE_REPO, "spellname.csv")
    #get_data_from_json("spell")
    merge_spell_data("spell","spellcasttimes")




main()