import json
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Function
from sqlparse.tokens import Keyword, DML
from tqdm import tqdm


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
        elif type(token) is Function and "COUNT" in token.value.upper():
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
            # Remove the WHERE keyword
            initial_clause = re.sub(r"(?i)^\s*WHERE\s*", "", str(token).strip())
            # remove trailing ? ? ?
            head_clause = re.sub(r'(\?\s*){3,}$', '', initial_clause)
            # Normalize WHERE clause (remove noise, abstract constants, and function calls)
            cleaned = re.sub(r'\s+\d+(\.\d+)?([Ee][-+]?\d+)?\s+\d+\s+\d+\s*$', '', head_clause)
            where_clause = re.sub(r"(0x[0-9a-fA-F]+)|(\b\d+(\.\d+)?([Ee][-+]?\d+)?\b)|('[^']*')", "?", cleaned)
        elif token.is_group:
            has_nested_queries = True

    # Post process projection (replace alias)
    final_columns = []
    for col in select_columns:
        for alias, table in alias_mapping.items():
            col = re.sub(rf"\b{alias}\.", f"{table}_", col)
        if "__Function_Call__" not in col:
            final_columns.append(col)
    # Create the resulting object
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
    # Post process where clause (replace alias), if any
    if where_clause:
        where_clause = where_clause.strip()
        for alias, table in alias_mapping.items():
            if table == "__Function_Call__":
                where_clause = re.sub(rf"\b{alias}\.[\w]+", "?", where_clause)
            else:
                where_clause = re.sub(rf"\b{alias}\.", f"{table}_", where_clause)
        parsed_query["filter"] = where_clause

    return parsed_query


def process_input(input_path, output_path):
    with open(input_path, mode="r", encoding="utf-8", errors="replace") as infile:
        # Remove header
        _ = next(infile)

        all_queries = []
        for line in tqdm(infile, desc=f"Parsing queries in {input_path}"):
            # Avoid processing the last empty line (or any other line without at least a SELECT)
            if len(line) > 6:
                info = extract_query_info(line)
                all_queries.append(info)

    print(f"\n💾 Saving queries to {output_path}")
    with open(output_path, "w", encoding="utf-8") as outfile:
        json.dump({"queries": all_queries}, outfile, ensure_ascii=False, indent=2)

    print(f"✅ Queries saved")
