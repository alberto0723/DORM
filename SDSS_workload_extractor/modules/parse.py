import json
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword, DML
from tqdm import tqdm


def preprocess_query_for_top_and_distinct(sql_query):
    top_regex = re.compile(r'\bTOP\s+(\d+)\b', re.IGNORECASE)
    distinct_regex = re.compile(r'\bDISTINCT\b', re.IGNORECASE)

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
    where_clause = ""
    has_nested_queries, has_group_by = False, False
    in_select, in_from = False, False

    for token in parsed.tokens:
        if token.ttype is DML and 'SELECT' in token.value.upper():
            in_select, in_from = True, False
        elif token.ttype is Keyword and token.value.upper() == 'FROM':
            in_select, in_from = False, True
        elif token.ttype is Keyword and token.value.upper() == 'GROUP BY':
            has_group_by = True
        elif isinstance(token, IdentifierList) or isinstance(token, Identifier):
            identifiers = token.get_identifiers() if isinstance(token, IdentifierList) else [token]
            for identifier in identifiers:
                if in_select:
                    col = str(identifier)
                    # Remove alias (find "AS")
                    if " AS " in col.upper():
                        col = re.split(r"\s+AS\s+", col, flags=re.IGNORECASE)[0]
                    # Split the column into several columns if it has algebraic operators (+,-,*,/), but it is not a SELECT *
                    if col[-1] != '*' and any(op in col for op in ['+', '-', '*', '/']):
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
                    match = re.match(r"([a-zA-Z_][\w]*)\s+(?:AS\s+)?([a-zA-Z_][\w]*)", name, flags=re.IGNORECASE)
                    if match:
                        # It is a table with alias
                        full_table, alias = match.groups()
                        alias_mapping[alias] = full_table
                        tables.append(full_table)
                    else:
                        # It still may be a function call
                        match = re.match(r"([a-zA-Z_][\w]*)\([^)]*\)\s+(?:AS\s+)?([a-zA-Z_][\w]*)", name, flags=re.IGNORECASE)
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
            where_clause = str(token).replace('\n', ' ')
            # Remove the WHERE keyword
            where_clause = re.sub(r"(?i)^\s*WHERE\s*", "", where_clause.strip())
            # Normalize WHERE clause (remove noise, abstract constants)
            cleaned = re.sub(r'\s+\d+(\.\d+)?([Ee][-+]?\d+)?\s+\d+\s+\d+\s*$', '', where_clause)
            normalized = re.sub(r"(0x[0-9a-fA-F]+)|(\b\d+(\.\d+)?([Ee][-+]?\d+)?\b)|('[^']*')", "?", cleaned)
            # remove trailing ? ? ? 
            where_clause = re.sub(r'(\?\s*){3,}$', '', normalized)

        elif token.is_group:
            has_nested_queries = True
    final_columns = []
    for col in select_columns:
        for alias, table in alias_mapping.items():
            col = re.sub(rf"\b{alias}\.", f"{table}_", col)
        final_columns.append(col)

    parsed_query = {
        "original_query": real_query,
        "pattern": sorted(set(tables)),
        "project": ["*"] if not final_columns else sorted(set(final_columns)),
        "has_distinct": distinct,
        "top_value": top_value,
        "has_order_by": has_order_by,
        "has_nested_queries": has_nested_queries,
        "has_group_by": has_group_by,
    }

    if where_clause:
        where_clause = where_clause.strip()
        for alias, table in alias_mapping.items():
            if table == "__Function_Call__":
                where_clause = re.sub(rf"\b{alias}\.[\w]+", f"?", where_clause)
            else:
                where_clause = re.sub(rf"\b{alias}\.", f"{table}_", where_clause)
        parsed_query["filter"] = where_clause

    return parsed_query


def process_input(input_path, output_path):
    with open(input_path, mode="r", encoding="utf-8", errors="replace") as infile:
        # Remove header
        _ = next(infile)

        all_queries = []
        for line in tqdm(infile, desc="Parsing queries"):
            # Avoid processing the last empty line
            if len(line) > 6:
                info = extract_query_info(line)
                all_queries.append(info)

    with open(output_path, "w", encoding="utf-8") as outfile:
        json.dump({"queries": all_queries}, outfile, ensure_ascii=False, indent=2)

    print(f"Parsed queries saved to {output_path}")
