import os
from pathlib import Path
import pandas as pd
from . import config


def custom_warning(message, category, filename, lineno, file=None, line=None):
    if config.show_warnings:
        print(f"{message} 👉 {os.path.basename(filename)}:{lineno}")


def custom_progress(message):
    if config.show_progress:
        print(message)


def extract_up_to_folder(path_str, folder_name) -> Path:
    path = Path(path_str).resolve()
    parts = path.parts
    if folder_name not in parts:
        raise ValueError(f"🚨 Folder '{folder_name}' not found in {path_str}")

    index = parts.index(folder_name)
    sub_path = Path(*parts[:index + 1])
    return sub_path


def drop_duplicates(dirty_list):
    unique_elems = []
    [unique_elems.append(elem) for elem in dirty_list if elem not in unique_elems]
    return unique_elems


def combine_buckets(patterns_list: list[list[str]]) -> list[list[str]]:
    '''
    Combines all lists of patterns in a smart way, by removing duplicates ASAP
    :param patterns_list:
    :return: list of combinations without duplicates
    '''
    if len(patterns_list) == 0:
        return [[]]
    else:
        current_pattern = patterns_list.pop(0)
        combinations = []
        for combination in combine_buckets(patterns_list):
            for current_table in current_pattern:
                if current_table in combination:
                    combinations.append(combination)
                else:
                    temp = combination + [current_table]
                    temp.sort()
                    combinations.append(temp)
        minimal_combinations = []
        for c1 in combinations:
            found = False
            for c2 in combinations:
                if c1 != c2 and all([t in c1 for t in c2]):
                    found = True
            if not found and c1 not in minimal_combinations:
                minimal_combinations.append(c1)
        return minimal_combinations


def df_difference(df1, df2):
    return pd.concat([df1, df2, df2], ignore_index=True).drop_duplicates(keep=False)


def read_db_conf(filename: str) -> dict[str, str]:
    path = Path(filename)
    if not path.is_file():
        raise FileNotFoundError(f"🚨 Database configuration file '{path.absolute()}' not found")
    try:
        parameters = {}
        # Read the database configuration from the provided txt file, line by line
        with open(path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                parameters[line.split('=', 1)[0]] = line.split('=', 1)[1].strip()
    except:
        raise ValueError(f"🚨 Database configuration file '{path.absolute()}' not properly formatted (check db_conf.example.txt).")
    return parameters
