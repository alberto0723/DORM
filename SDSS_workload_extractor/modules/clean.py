import json
import re
from tqdm import tqdm

def is_mydb_query(query_text):
    """
    Returns True if the query should be discarded due to referencing MyDB
    in a FROM, JOIN, or UPDATE clause. Allows SELECT INTO MyDB.
    """
    query_text = query_text.lower()

    # Keep if it's just SELECT INTO MyDB (public data stored into MyDB)
    if re.search(r'select\s+into\s+mydb\.', query_text):
        return False

    # Discard if MyDB is in FROM, JOIN, or UPDATE (private data access)
    patterns = [
        r'from\s+mydb\.',       # FROM MyDB...
        r'join\s+mydb\.',       # JOIN MyDB...
        r'update\s+mydb\.',     # UPDATE MyDB...
        r'delete\s+from\s+mydb\.' # DELETE FROM MyDB...
    ]
    
    for pattern in patterns:
        if re.search(pattern, query_text):
            return True

    return False  # Otherwise, keep

def clean_queries(input_file, output_file):
    print(f"Loading parsed data from {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as infile:
        data = json.load(infile)

    print("🧹 Filtering out queries that read or modify MyDB...")
    cleaned = [q for q in tqdm(data["queries"], desc="Filtering queries") if not is_mydb_query(q["original_query"])]

    print(f"✅ Cleaned: {len(cleaned)} of {len(data["queries"])} queries retained.")
    
    print(f"💾 Saving cleaned data to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as outfile:
        json.dump(cleaned, outfile, indent=2, ensure_ascii=False)
    print("Done!")
