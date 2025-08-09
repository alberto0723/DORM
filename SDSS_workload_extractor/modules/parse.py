import json
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Function, Comparison
from sqlparse.tokens import Keyword, DML, Token
from tqdm import tqdm
from pathlib import Path


def is_discarded_query(query_text):
    """
    Returns True if the query should be discarded due to referencing MyDB
    in a FROM, JOIN, or UPDATE clause. Allows SELECT INTO MyDB.
    """
    query_text = query_text.lower()

    # Keep if it's just SELECT INTO MyDB (public data stored into MyDB)
    # Discard if MyDB is in FROM, JOIN, or UPDATE (private data access)
    patterns = [
        r'from\s+mydb\.',  # FROM MyDB...
        r'join\s+mydb\.',  # JOIN MyDB...
        r'update\s+mydb\.',  # UPDATE MyDB...
        r'delete\s+from\s+mydb\.'  # DELETE FROM MyDB...
    ]
    # Discard also queries using metadata in dbobjects
    patterns.append('dbobjects')

    for pattern in patterns:
        if re.search(pattern, query_text):
            return True
    return False  # Otherwise, keep

def preprocess_query_for_top_and_distinct(sql_query):
    top_regex = re.compile(r'\bTOP\s+(\d+)\b', re.IGNORECASE)
    distinct_regex = re.compile(r'\bDISTINCT\b', re.IGNORECASE)

    sql_query = sql_query.strip()
    top_match = top_regex.search(sql_query)
    top_value = top_match.group(1) if top_match else None
    sql_query = top_regex.sub('', sql_query)

    distinct = bool(distinct_regex.search(sql_query))
    sql_query = distinct_regex.sub('', sql_query)

    return sql_query, top_value, distinct


def extract_query_info(real_query):
    sql_query, top_value, distinct = preprocess_query_for_top_and_distinct(real_query)
    has_order_by = bool(re.search(r'\bORDER\s+BY\b', sql_query, re.IGNORECASE))
    parsed_results = sqlparse.parse(sql_query)
    parsed = parsed_results[0]

    select_columns, tables = [], []
    alias_mapping = {}
    comparisons = []
    has_nested_queries, has_group_by = False, False
    in_select, in_from = False, False

    for token in parsed.tokens:
        if token.ttype is DML and 'SELECT' in token.value.upper():
            in_select, in_from = True, False
        elif token.ttype is Keyword and token.value.upper() == 'FROM':
            in_select, in_from = False, True
        elif token.ttype is Keyword and token.value.upper() == 'GROUP BY':
            has_group_by = True
        elif isinstance(token, Function) and "COUNT" in token.value.upper():
            # The parser detects sometimes the count as a function and sometimes as an identifier (may depend on having an alias)
            if in_select:
                select_columns.append("count(*)")
        elif isinstance(token, IdentifierList) or isinstance(token, Identifier):
            identifiers = token.get_identifiers() if isinstance(token, IdentifierList) else [token]
            for identifier in identifiers:
                if in_select:
                    col = str(identifier)
                    # TODO: Extract the attributes inside HTML code (by now, they are simply ignored)
                    if "HREF=HTTP" not in col.upper():
                        # Remove alias (find "AS")
                        if " AS " in col.upper():
                            col = re.split(r"\s+AS\s+", col, flags=re.IGNORECASE)[0]
                        # The parser detects sometimes the count as a function and sometimes as an identifier (may depend on having an alias)
                        if "COUNT" in col.upper():
                            col = "count(*)"
                        # Split the column into several columns if it has algebraic operators (+,-,*,/), but it is not a SELECT *
                        elif col[-1] != '*' and any(op in col for op in ['+', '-', '*', '/']):
                            col_parts = re.split(r'\s*[\+\-\*/]\s*', col)
                            for part in col_parts:
                                cleaned_part = part.strip(" ()")  # Remove whitespace and parentheses
                                # Only keep if it looks like a column (no literals like 1.21 or 'text')
                                if re.match(r'^[a-zA-Z_][\w\.]*$', cleaned_part):
                                    select_columns.append(cleaned_part)
                            continue  # Skip appending the full col again
                        select_columns.append(col)
                elif in_from:
                    name = str(identifier).strip()

                    # Try to match "TableName AS alias" or "TableName alias"
                    # Some temporary tables start with "#"
                    match = re.match(r"(#?[a-zA-Z_][\w]*)\s+(?:AS\s+)?([a-zA-Z_][\w]*)", name, flags=re.IGNORECASE)
                    if match:
                        # It is a table with alias
                        full_table, alias = match.groups()
                        alias_mapping[alias] = full_table
                        tables.append(full_table)
                    else:
                        # It still may be a function call, that optionally starts by "bdo."
                        match = re.match(r"(?:dbo\.)?([a-zA-Z_][\w]*)\([^)]*\)\s+(?:AS\s+)?([a-zA-Z_][\w]*)", name, flags=re.IGNORECASE)
                        if match:
                            _, alias = match.groups()
                            alias_mapping[alias] = "__Function_Call__"
                        else:
                            # TODO: Check if it is a single table and prepend the table name to all attributes both in select and where clauses (even if no alias was used)
                            #       Something like "alias_mapping[full_table] = full_table" could be used to simplify the processing
                            # Try to use alias if already in mapping (e.g., JOIN f)
                            parts = name.split()
                            for part in parts:
                                if part in alias_mapping:
                                    tables.append(alias_mapping[part])
                                    break
                            # Otherwise, if it's just a table name
                            if re.match(r"^[a-zA-Z_][\w]*$", name) and name.lower() not in [
                                "select", "from", "where", "join", "on", "as", "type", "name"
                            ]:
                                tables.append(name)
        elif isinstance(token, Where):
            for i in range(len(token.tokens)):
                current = token.tokens[i]
                if isinstance(current, Comparison):
                    attributes = 0
                    for elem in current.tokens:
                        if elem.ttype == Token.Operator.Comparison:
                            operator = elem.value
                        elif isinstance(elem, Identifier):
                            attribute = elem.value
                            for alias, table in alias_mapping.items():
                                if table == "__Function_Call__":
                                    attribute = re.sub(rf"\b{alias}\.[\w]+", "?", attribute)
                            # If it is not a function call, then we have an attribute
                            if attribute != "?":
                                attributes += 1
                    if attributes == 1:
                        comparisons.append({"attribute": attribute, "operator": operator})
                elif current.ttype is Keyword and current.value.upper() == "BETWEEN":
                    operator = current.value
                    assert i > 2, f"Wrong 'between' clause in {token}"
                    attribute = token.tokens[i-2].value
                    comparisons.append({"attribute": attribute, "operator": operator})
                elif current.ttype is Keyword:
                    if current.value.upper() not in ("WHERE", " "):
                        logic_word = current.value
                        assert logic_word.upper() == "AND", "Non conjunctive query: " + logic_word

        elif token.is_group:
            has_nested_queries = True

    # Create the resulting object
    parsed_query = {
        "original_query": real_query,
        "pattern": tables,
        "project": ["*"] if not select_columns else select_columns,
        "filter_clauses": comparisons,
        "has_distinct": distinct,
        "top_value": top_value,
        "has_order_by": has_order_by,
        "has_nested_queries": has_nested_queries,
        "has_group_by": has_group_by,
        "alias_mapping": alias_mapping
    }
    return parsed_query


def post_processing(parsed_query):
    # Post process tables list
    parsed_query["pattern"] = sorted(set(parsed_query["pattern"]))

    # Post process projection (replace alias)
    final_columns = []
    for col in sorted(set(parsed_query["project"])):
        if len(parsed_query["pattern"]) == 1 and "." not in col:
            col = parsed_query["pattern"][0]+"_"+col
        else:
            for alias, table in parsed_query.get("alias_mapping", {}).items():
                col = re.sub(rf"\b{alias}\.", f"{table}_", col)
        if "__Function_Call__" not in col:
            final_columns.append(col)
    parsed_query["project"] = final_columns

    # Post process filter clauses
    final_comparisons = []
    for comparison in parsed_query["filter_clauses"]:
        if len(parsed_query["pattern"]) == 1 and "." not in comparison["attribute"]:
            comparison["attribute"] = parsed_query["pattern"][0]+"_"+comparison["attribute"]
        else:
            for alias, table in parsed_query.get("alias_mapping", {}).items():
                comparison["attribute"] = re.sub(rf"\b{alias}\.", f"{table}_", comparison["attribute"])
        final_comparisons.append(comparison)
    parsed_query["filter_clauses"] = sorted(final_comparisons, key=lambda c: c["attribute"])

    return parsed_query


def process_input(input_path, output_path):
    with open(input_path, mode="r", encoding="utf-8", errors="replace") as infile, \
      open(Path(output_path).joinpath("discarded.txt"), mode="w", encoding="utf-8") as discardsfile:
        # Remove header
        _ = next(infile)

        all_queries = []
        discarded = 0
        for line in tqdm(infile, desc=f"Parsing queries in {input_path}"):
            # Avoid processing the last empty line (or any other line without at least a SELECT)
            if len(line) > 6:
                # Discarding queries that read or modify MyDB, or use 'dbobjects'
                if is_discarded_query(line):
                    discardsfile.write(line)
                    discarded += 1
                else:
                    query = extract_query_info(line)
                    # Discard have temporary tables starting with '#' in the FROM, or have an empty SELECT clause
                    # TODO: Check if it is really necessary to define "table_set"
                    table_set = [t.split()[0].lower() for t in query.get("pattern", []) if "(" not in t]
                    if table_set and all(t[0] != '#' for t in table_set) and query.get("project", []):
                        query = post_processing(query)
                        all_queries.append(query)
                    else:
                        discardsfile.write(line)
                        discarded += 1

    print(f"\n💾 Saving {len(all_queries)} queries to {output_path}")
    with open(Path(output_path).joinpath("parsed.json"), "w", encoding="utf-8") as outfile:
        json.dump({"queries": all_queries}, outfile, ensure_ascii=False, indent=2)

    print(f"✅ Queries saved")
