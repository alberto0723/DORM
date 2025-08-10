import json
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Function, Comparison
from sqlparse.tokens import Keyword, DML, Token
from tqdm import tqdm
from pathlib import Path


select_regex = re.compile(r'\bSELECT\b', re.IGNORECASE)
top_regex = re.compile(r'\bTOP\s+(\d+)\b', re.IGNORECASE)
distinct_regex = re.compile(r'\bDISTINCT\b', re.IGNORECASE)
match_regex = re.compile(r'\bMATCH\b', re.IGNORECASE)
type_regex = re.compile(r'\bTYPE\b', re.IGNORECASE)
class_regex = re.compile(r'\bCLASS\b', re.IGNORECASE)
mode_regex = re.compile(r'\bMODE\b', re.IGNORECASE)
version_regex = re.compile(r'\bVERSION\b', re.IGNORECASE)
cycle_regex = re.compile(r'\bCYCLE\b', re.IGNORECASE)


def is_discarded_query(query_text):
    # Check if it is a single select clause (without subqueries)
    if len(select_regex.findall(query_text)) != 1:
        return True

    query_text = query_text.lower()

    # Keep if it's just SELECT INTO MyDB (public data stored into MyDB)
    # Discard if MyDB is in FROM, JOIN, or UPDATE (private data access)
    patterns = [
        r'from\s+mydb\.',  # FROM MyDB...
        r'join\s+mydb\.',  # JOIN MyDB...
        r'update\s+mydb\.',  # UPDATE MyDB...
        r'delete\s+from\s+mydb\.'  # DELETE FROM MyDB...
    ]
    # Discard also queries using metadata
    patterns.extend(['dbobjects', 'sqllog', 'dbcolumns', 'information_schema'])
    # Remove also other SQL commands
    patterns.extend(['exec', r'create\s+table'])

    for pattern in patterns:
        if re.search(pattern, query_text):
            return True
    return False  # Otherwise, keep

def preprocess_query_for_match_top_and_distinct(sql_query):
    sql_query = sql_query.strip()
    top_match = top_regex.search(sql_query)
    top_value = top_match.group(1) if top_match else None
    sql_query = top_regex.sub('', sql_query)

    distinct = bool(distinct_regex.search(sql_query))
    sql_query = distinct_regex.sub('', sql_query)

    # This is necessary, because there are some table or attribute names in SDSS that coincide with reserved sql keywords, and it confuses the SQL parser
    # As soon as the keywords are not really present in the queries, this should work
    # TODO: Change the parsing library
    sql_query = match_regex.sub('__MATCH__', sql_query)
    sql_query = type_regex.sub('__TYPE__', sql_query)
    sql_query = class_regex.sub('__CLASS__', sql_query)
    sql_query = mode_regex.sub('__MODE__', sql_query)
    sql_query = version_regex.sub('__VERSION__', sql_query)
    sql_query = cycle_regex.sub('__CYCLE__', sql_query)

    return sql_query, top_value, distinct


def extract_query_info(real_query):
    sql_query, top_value, distinct = preprocess_query_for_match_top_and_distinct(real_query)
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
            identifiers = list(token.get_identifiers()) if isinstance(token, IdentifierList) else [token]
            # We need to remove comparisons, because in the case of using "ON" to express a join condition, this appears in the list of identifiers
            identifiers = [i for i in identifiers if not isinstance(i, Comparison)]
            for i in range(len(identifiers)):
                identifier = identifiers[i]
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
                        if full_table == "__MATCH__":
                            full_table = "Match"
                        alias_mapping[alias] = full_table
                        tables.append(full_table)
                    else:
                        # It still may be a function call with an alias, that optionally starts by "bdo."
                        match = re.match(r"(?:dbo\.)?([a-zA-Z_][\w]*)\s*\([^)]*\)\s+(?:AS\s+)?([a-zA-Z_][\w]*)", name, flags=re.IGNORECASE)
                        if match:
                            _, alias = match.groups()
                            alias_mapping[alias] = "__Function_Call__"
                        else:
                            # It could also be a function call without alias
                            match = re.match(r"(?:dbo\.)?([a-zA-Z_][\w]*)\s*\([^)]*\)\s*", name, flags=re.IGNORECASE)
                            if not match:
                                if identifier.value == "__MATCH__":
                                    tables.append("Match")
                                else:
                                    tables.append(identifier.value)
        elif isinstance(token, Where):
            in_from = False
            for i in range(len(token.tokens)):
                current = token.tokens[i]
                if isinstance(current, Comparison):
                    attributes = []
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
                                attributes.append(attribute)
                    if len(attributes) == 1:
                        comparisons.append({"attribute": attributes[0], "operator": operator})
                elif current.ttype is Keyword and current.value.upper() == "BETWEEN":
                    operator = current.value
                    assert i > 2, f"Wrong 'between' clause in {token}"
                    attribute = token.tokens[i-2].value
                    comparisons.append({"attribute": attribute, "operator": operator})
                elif current.ttype is Keyword:
                    # TODO: Consider other more complex comparisons not properly treated by sqlparse library
                    if current.value.upper() not in ("WHERE", "NOT", "IN", "OR", "IS", "NULL", "NOT NULL", "GO", "ABS"):
                        logic_word = current.value
                        assert logic_word.upper() == "AND", "Non conjunctive query: '" + logic_word + f"' in {sql_query}"

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
    }
    return parsed_query, alias_mapping


def post_processing(parsed_query, alias_mapping):
    # Post process tables list
    parsed_query["pattern"] = sorted(set(parsed_query["pattern"]))

    # Post process projection (replace alias)
    final_columns = []
    star_found = False
    for col in sorted(set(parsed_query["project"])):
        if col[-1] == "*":
            star_found = True
        if len(parsed_query["pattern"]) == 1 and "." not in col:
            col = parsed_query["pattern"][0]+"_"+col
        else:
            for alias, table in alias_mapping.items():
                col = re.sub(rf"\b{alias}\.", f"{table}_", col)
        if "__Function_Call__" not in col:
            col = re.sub(r"__TYPE__", "type", col)
            col = re.sub(r"__CLASS__", "class", col)
            col = re.sub(r"__MODE__", "mode", col)
            col = re.sub(r"__VERSION__", "version", col)
            col = re.sub(r"__CYCLE__", "cycle", col)
            final_columns.append(col)
    if star_found and len(parsed_query["pattern"]):
        parsed_query["project"] = ["*"]
    else:
        parsed_query["project"] = final_columns

    # Post process filter clauses
    final_comparisons = []
    for comparison in parsed_query["filter_clauses"]:
        if len(parsed_query["pattern"]) == 1 and "." not in comparison["attribute"]:
            comparison["attribute"] = parsed_query["pattern"][0]+"_"+comparison["attribute"]
        else:
            for alias, table in alias_mapping.items():
                comparison["attribute"] = re.sub(rf"\b{alias}\.", f"{table}_", comparison["attribute"])
        comparison["attribute"] = re.sub(r"__TYPE__", "type", comparison["attribute"])
        comparison["attribute"] = re.sub(r"__CLASS__", "class", comparison["attribute"])
        comparison["attribute"] = re.sub(r"__MODE__", "mode", comparison["attribute"])
        comparison["attribute"] = re.sub(r"__VERSION__", "version", comparison["attribute"])
        comparison["attribute"] = re.sub(r"__CYCLE__", "cycle", comparison["attribute"])
        final_comparisons.append(comparison)
    parsed_query["filter_clauses"] = sorted(final_comparisons, key=lambda c: c["attribute"])

    if parsed_query["project"] == ["*"]:
        parsed_query["all_attributes"] = ["*"]
    else:
        parsed_query["all_attributes"] = sorted(set(parsed_query["project"]+[c["attribute"] for c in parsed_query["filter_clauses"]]))

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
                # Discarding queries that read or modify MyDB, or use system tables like 'dbobjects'
                if is_discarded_query(line):
                    discardsfile.write(line)
                    discarded += 1
                else:
                    query, alias_mapping = extract_query_info(line)
                    # Discard have temporary tables starting with '#' in the FROM, or have an empty SELECT clause
                    if query.get("project") and query.get("pattern") and all(t[0] != '#' for t in query.get("pattern")):
                        query = post_processing(query, alias_mapping)
                        all_queries.append(query)
                    else:
                        discardsfile.write(line)
                        discarded += 1

    output_file = Path(output_path).joinpath("parsed.json")
    print(f"\n💾 Saving {len(all_queries)} queries to {output_file}")
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump({"queries": all_queries}, outfile, ensure_ascii=False, indent=2)

    print(f"✅ Queries saved")
