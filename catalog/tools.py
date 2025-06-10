import os
from pathlib import Path
import pandas as pd
from . import config


def custom_warning(message, category, filename, lineno, file=None, line=None):
    if config.show_warnings:
        print(f"{message} 👉 {os.path.basename(filename)}:{lineno}")


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
                if current_table not in combination:
                    temp = combination + [current_table]
                    temp.sort()
                    combinations.append(temp)
                else:
                    combinations.append(combination)
        return drop_duplicates(combinations)


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
