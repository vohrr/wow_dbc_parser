from files import BASE_SPELL, SPELL_FILES, SPELL_FILES_OPTIONAL, TALENT_FILES
import requests
import csv
import json
import os
import pandas
from pathlib import Path

BETA_REPO = 'https://raw.githubusercontent.com/Vladinator/wow-dbc-archive/release/wow_beta_latest'
LIVE_REPO = 'https://raw.githubusercontent.com/Vladinator/wow-dbc-archive/release/wow_latest'
PTR_REPO = 'https://raw.githubusercontent.com/Vladinator/wow-dbc-archive/release/wow_ptr_latest'

DESIRED_SPELL_IDS = [115151, 116670, 119611, 107428, 124682, 117907, 388023, 322118, 115175, 116849]

# Base directory of this script (ensures paths work regardless of CWD)
BASE_DIR = Path(__file__).parent

def get_file_from_repo(
    version: str,
    file_name: str,
    folder_name: str,
    filter_column: str | None = None,
    filter_values: list[int] | None = None,
):
    requestUrl = f"{version}/{file_name}.csv"
    
    print(f"Fetching data from {file_name}...")
    
    out_dir = BASE_DIR / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{file_name.replace('.csv','')}.json"
    with open(out_path, "w", encoding="utf-8") as jsonfile:
       
        with (requests.get(requestUrl, stream=True)) as response:
            
            if response.ok:
                lines = (line.decode('utf-8') for line in response.iter_lines())
                data = []
                desired_set = set(filter_values) if filter_column and filter_values else None
                for row in csv.DictReader(lines):
                    # If filtering is requested, skip rows not in the desired set
                    if desired_set is not None:
                        raw_val = row.get(filter_column)
                        if raw_val is None or raw_val == "":
                            continue
                        try:
                            key_val = int(raw_val)
                        except Exception:
                            # Non-numeric value where numeric expected; skip
                            continue
                        if key_val not in desired_set:
                            continue
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
                try:
                    os.remove(out_path)
                except FileNotFoundError:
                    pass
                
                print(f"Error retrieving data from {file_name}. Response: {response.status_code} {response.reason}")


def get_data_from_json(file_name:str, folder:str):    
    file_path = get_json_file_name(file_name, folder)
    
    try:
        file_data = json.load(open(file_path)) 
        for item in range(0,1):
            print(file_data[item])
    
    except FileNotFoundError as exception:
        print(f"Error: {file_name} not found. {exception}")

def merge_spell_data(base_name: str, base_column: str, merge_name: str, merge_column: str):
    print (f"Start merge {merge_name} into {base_name}")
    base_file = get_json_file_name(base_name, 'merged_json')
    merge_file = get_json_file_name(merge_name, 'json')
    merged = {}
    print(f"Reading {base_name}...")
    
    base_data =  pandas.read_json(base_file, orient='records')
    print(f"Done. Reading {merge_name}...")
    
    merge_data = pandas.read_json(merge_file, orient='records')
    # Skip empty side tables cleanly
    if merge_data is None or merge_data.empty:
        print(f"Info: '{merge_name}' has no rows after filtering; skipping.")
        return
    # Normalize key column to 'ID' for consistent joins
    if merge_column != 'ID':
        # If the incoming frame already has an 'ID' (row id), preserve it under a different name
        if 'ID' in merge_data.columns:
            merge_data = merge_data.rename(columns={'ID': f'{merge_name}_RowID'})
        if merge_column in merge_data.columns:
            merge_data = merge_data.rename(columns={merge_column: 'ID'})
        else:
            print(f"Warning: merge column '{merge_column}' not found in {merge_name}; skipping merge.")
            return

    print("Done. Merging...")
    how_join = 'left'  # preserve all base spells

    if merge_column != 'ID':
        # One-to-many: group side rows into a list of dicts per ID and attach as nested array
        grouped_series = merge_data.groupby('ID').apply(
            lambda g: g.drop(columns=['ID'], errors='ignore').to_dict('records')
        )
        grouped = grouped_series.reset_index(name=merge_name)

        merged = pandas.merge(
            base_data,
            grouped,
            how=how_join,
            left_on=base_column,
            right_on='ID',
        )

        # Clean duplicate key columns
        if 'ID_x' in merged.columns:
            merged = merged.rename(columns={'ID_x': 'ID'})
        if 'ID_y' in merged.columns:
            merged = merged.drop(columns=['ID_y'])
    else:
        # One-to-one (expected): remove overlapping columns to avoid suffix collisions
        overlapping = [c for c in merge_data.columns if c in base_data.columns and c != 'ID']
        if overlapping:
            merge_data = merge_data.drop(columns=overlapping)

        # If the side has duplicates by ID, collapse to first row
        if 'ID' in merge_data.columns and merge_data.duplicated(subset=['ID']).any():
            merge_data = merge_data.sort_values('ID').drop_duplicates(subset=['ID'], keep='first')

        merged = pandas.merge(
            base_data,
            merge_data,
            how=how_join,
            left_on=base_column,
            right_on='ID',
            suffixes=("", f"__{merge_name}")
        )

        # Clean duplicate key columns
        if 'ID_x' in merged.columns:
            merged = merged.rename(columns={'ID_x': 'ID'})
        if 'ID_y' in merged.columns:
            merged = merged.drop(columns=['ID_y'])
    
    print(f"Successfully merged {base_name} and {merge_name}")

    spell_json_file = str(BASE_DIR / "merged_json" / "spell.json")

    # Ensure unique rows per spell ID; keep first occurrence if one-to-many created duplicates
    def _ensure_unique_ids(df: pandas.DataFrame, id_col: str = 'ID') -> pandas.DataFrame:
        if id_col in df.columns:
            dup_mask = df.duplicated(subset=[id_col], keep=False)
            if dup_mask.any():
                dup_counts = (
                    df.loc[dup_mask, [id_col]]
                    .value_counts()
                    .to_dict()
                )
                print(
                    f"Warning: duplicate IDs detected after merging '{merge_name}': "
                    f"{len(dup_counts)} IDs; deduplicating by keeping first occurrence."
                )
            return df.drop_duplicates(subset=[id_col], keep='first').reset_index(drop=True)
        else:
            return df

    merged = _ensure_unique_ids(merged, 'ID')

    merged.to_json(spell_json_file, orient='records')

def get_json_file_name(name: str, folder: str) -> str:
    path = BASE_DIR / folder / f"{name}.json"
    # Ensure parent exists for any writes
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)

def main():
    # Fetch base spells filtered by desired IDs
    get_file_from_repo(
        LIVE_REPO,
        BASE_SPELL[0],
        'merged_json',
        filter_column=BASE_SPELL[1],
        filter_values=DESIRED_SPELL_IDS,
    )
       
    # Fetch and merge each side table filtered by its join column
    for file_to_merge in SPELL_FILES:
        name, join_col = file_to_merge
        get_file_from_repo(
            LIVE_REPO,
            name,
            'json',
            filter_column=join_col if join_col else None,
            filter_values=DESIRED_SPELL_IDS if join_col else None,
        )
        merge_spell_data(BASE_SPELL[0], BASE_SPELL[1], name, join_col)

    # Fetch talents filtered by SPELLID
    get_file_from_repo(
        LIVE_REPO,
        TALENT_FILES[0],
        'json',
        filter_column=TALENT_FILES[1],
        filter_values=DESIRED_SPELL_IDS,
    )

main()

#get_data_from_json('spell', 'merged_json')