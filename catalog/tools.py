import pandas as pd


def drop_duplicates(dirty_list):
    unique_elems = []
    [unique_elems.append(elem) for elem in dirty_list if elem not in unique_elems]
    return unique_elems


def combine_tables(patterns_list):
    if len(patterns_list) == 0:
        return [[]]
    else:
        current_pattern = patterns_list.pop(0)
        combinations = []
        for combination in combine_tables(patterns_list):
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
