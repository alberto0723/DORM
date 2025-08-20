import json
from collections import defaultdict
from tqdm import tqdm
from pathlib import Path


total_queries = 0


# Step 0: Load the parsed queries from input JSON
def load_parsed_queries(path: str) -> list:
    input_file = Path(path).joinpath("parsed.json")
    print(f"📂 Loading filtered and parsed queries from {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        raw = json.load(f)
        queries = raw["queries"] if isinstance(raw, dict) else raw
    print(f"✅ Loaded {len(queries)} queries.\n")
    return queries


# Step 1: Group queries by table and modifiers
def group_queries_by_table(queries: list, modifiers: list) -> dict:

    table_groups = defaultdict(list)
    global total_queries
    for query in tqdm(queries, desc="🔧 Grouping queries by table combinations and modifiers"):
        table_set = tuple(sorted(set(
            t.lower().split()[0] for t in query.get("pattern", []) if "(" not in t
        )))
        group_key = [table_set]
        if "distinct" in modifiers and "has_distinct" in query:
            group_key.append(query["has_distinct"])
        if "groupby" in modifiers and "has_group_by" in query:
            group_key.append(query["has_group_by"])
        if "orderby" in modifiers and "has_order_by" in query:
            group_key.append(query["has_order_by"])
        if "top" in modifiers and "top_value" in query and query["top_value"] is not None:
            group_key.append(query["top_value"])

        table_groups[tuple(group_key)].append(query)
        total_queries += 1

    print(f"✅ Created {len(table_groups)} groups with a total of {total_queries} queries.\n")
    return dict(table_groups)


# Step 2: Frequency analysis with Jaccard merging
def calculate_column_frequencies(grouped_queries: dict, modifiers: list, threshold_ratio: float, jaccard_threshold: float = 0.8) -> list[dict]:

    def stringify_key(key):
        if isinstance(key, (tuple, list)):
            return ", ".join(stringify_key(k) for k in key)
        return str(key)

    global total_queries
    modifier_map = {
        "distinct": "has_distinct",
        "top": "top_value",
        "orderby": "has_order_by",
        "groupby": "has_group_by",
    }
    min_queries = total_queries * threshold_ratio

    summarized_groups = []
    for table_key, queries in tqdm(grouped_queries.items(), desc="🔍 Grouping queries and filtering them by frequency"):
        # All the group must have the minimum queries, so that a part of it can have it
        if len(queries) > min_queries:
            # We split the queries depending on the select having a star or not
            star_queries = []
            non_star_queries = []
            # Since we do not have information on the schema of the tables, we assume '*' is simply the union of all attributes appearing in the queries
            all_columns = set()
            for query in queries:
                columns = query.get("all_attributes")
                if len(columns) > 1 or columns[0][-1] != '*':
                    all_columns |= set(columns)
                    non_star_queries.append(query)
                else:
                    star_queries.append(query)
            # Merge back all the queries sorted by the length of the projection
            queries = star_queries + sorted(non_star_queries, key=lambda q: len(q.get("all_attributes")), reverse=True)
            # After sorting, we generate an auxiliary structure with the count of columns of each query
            pattern_counts = defaultdict(int)
            pattern_representative = {}
            pattern_conditions = {}
            for query in queries:
                columns = query.get("all_attributes", [])
                # Columns need to be previously sorted on parsing
                column_key = tuple(columns)
                pattern_counts[column_key] += 1
                if column_key not in pattern_representative:
                    pattern_representative[column_key] = query
                    pattern_conditions[column_key] = query.get("filter_clauses")
                else:
                    pattern_conditions[column_key] = [c for c in pattern_conditions[column_key] if c in query.get("filter_clauses")]

            used = set()
            patterns = list(pattern_counts.items())

            for i in range(len(patterns)):
                if i not in used:
                    shape_i, count_i = patterns[i]
                    if shape_i[0][0] == '*':
                        star_found = True
                        set_i = all_columns
                    else:
                        star_found = False
                        set_i = set(shape_i)
                    merged = set_i.copy()
                    intersect_conditions = pattern_conditions[shape_i]
                    total_count = count_i

                    for j in range(i + 1, len(patterns)):
                        if j not in used:
                            shape_j, count_j = patterns[j]
                            if shape_j[0][0] == '*':
                                star_found = True
                                set_j = all_columns
                            else:
                                set_j = set(shape_j)
                            jaccard = len(set_i & set_j) / len(set_i | set_j)
                            if jaccard >= jaccard_threshold:
                                used.add(j)
                                merged |= set_j
                                # Keep the intersection of all the conditions of queries in the group
                                intersect_conditions = [c for c in intersect_conditions if c in pattern_conditions[shape_j]]
                                total_count += count_j

                    filter_clauses = []
                    i = 1
                    for c in intersect_conditions:
                        if c["operator"].upper() == "BETWEEN":
                            filter_clauses.append(c["attribute"]+" BETWEEN $"+str(i)+" AND $"+str(i+1))
                            i += 2
                        else:
                            filter_clauses.append(c["attribute"] + c["operator"] + "$" + str(i))
                            i += 1
                    if total_count > min_queries:
                        # We are assuming conjunctive queries in the filter
                        group_summary = {
                            "frequency": total_count / total_queries,
                            "original_query": pattern_representative[shape_i].get("original_query", ""),
                            "pattern": pattern_representative[shape_i].get("pattern", []),
                            "project": sorted(merged) if not star_found else ['*'],
                            "filter": filter_clauses,
                            "has_nested_queries": pattern_representative[shape_i].get("has_nested_queries"),
                        }
                        # Add only the modifiers used in grouping
                        for mod in modifiers:
                            json_key = modifier_map.get(mod)
                            if json_key in queries[i]:
                                group_summary[json_key] = queries[i][json_key]
                        summarized_groups.append(group_summary)
    print(f"✅ Retained {len(summarized_groups)} groups")
    return summarized_groups


# Step 3: Post-process queries with hardcoded rules
def post_process(summarized_groups: list[dict]) -> list[dict]:
    def clean_attr(c):
        new_c = None
        for key, value in hierarchy_top.items():
            if c.startswith(key.lower() + "_"):
                new_c = c.replace(key.lower(), value.lower(), 1)  # Replace only the first occurrence
        if new_c is not None:
            return new_c
        else:
            return c

    associations = ["SpecObjAll_galSpecExtra", "SpecObjAll_zooSpec", "SpecObjAll_galSpecIndx", "SpecObjAll_PlateX", "SpecObjAll_PhotoObjAll", "Field_Frame", "PhotoObjAll_Photoz", "PhotoObjAll_Field"]
    hierarchy_top = {
        "PhotoObj": "PhotoObjAll",
        "PrimaryObj": "PhotoObjAll",
        "SpecObj": "SpecObjAll"
    }
    for group in summarized_groups:
        # Add associations to the pattern
        for p1 in group["pattern"]:
            for p2 in group["pattern"]:
                if p1 in hierarchy_top:
                    lhs = hierarchy_top[p1]
                else:
                    lhs = p1
                if p2 in hierarchy_top:
                    rhs = hierarchy_top[p2]
                else:
                    rhs = p2
                if lhs+"_"+rhs in associations:
                    group["pattern"].append(lhs+"_"+rhs)

        # Change names to the top of the hierarchy in projected attributes
        translated_projection = []
        for a in group["project"]:
            translated_projection.append(clean_attr(a))
        group["project"] = translated_projection

        # Change names to the top of the hierarchy in filter attributes
        translated_filter_clauses = []
        for c in group["filter"]:
            translated_filter_clauses.append(clean_attr(c))
        group["filter"] = " and ".join(translated_filter_clauses)

    return summarized_groups


# Step 4: Save summarized representative queries per group
def save_grouped_queries(summarized_groups: list[dict], output_path: str):
    # Sort the result by frequencies and generate ids
    sorted_groups = []
    for group in sorted(summarized_groups, key=lambda g: g.get("frequency"), reverse=True):
        group["group_id"] = len(sorted_groups) + 1
        sorted_groups.append(group)

    output_file = Path(output_path).joinpath("queries.json")
    print(f"\n💾 Saving summarized group representatives to {output_file}...")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(
                {
                    "total_queries": total_queries,
                    "query_frequency_in_workload": total_queries / (30*24*60*60),  # This assumes a month of 30 days
                    "queries": sorted_groups
                }, f, indent=2, ensure_ascii=False)
        print(f"✅ Data saved")
    except Exception as e:
        print(f"❌ Failed to save grouped queries: {e}")
