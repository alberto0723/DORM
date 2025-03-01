import pandas as pd

def df_difference(df1, df2):
    return pd.concat([df1, df2, df2], ignore_index=True).drop_duplicates(keep=False)
