import json
from collections import defaultdict
from tqdm import tqdm


total_queries = 0


# Step 0: Load the cleaned queries from input JSON
def load_cleaned_queries(path: str) -> list:
    print(f"📂 Loading cleaned queries from {path}...")
    with open(path, 'r', encoding='utf-8') as f:
        raw = json.load(f)
        queries = raw["queries"] if isinstance(raw, dict) else raw
    print(f"✅ Loaded {len(queries)} queries.\n")
    return queries


# Step 1: Group queries by table and modifiers
def group_queries_by_table(queries: list, modifiers: list) -> dict:
    print("🔧 Grouping queries by table combinations and modifiers...")

    table_groups = defaultdict(list)
    global total_queries
    for query in tqdm(queries, desc="📊 Building groups"):
        tables = query.get("pattern", [])
        if tables:
            table_set = tuple(sorted(set(
                t.lower().split()[0] for t in tables if "(" not in t
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

    for i, (_, group_queries) in enumerate(table_groups.items()):
        for q in group_queries:
            q["group_id"] = i

    print(f"✅ Created {len(table_groups)} groups with a total of {total_queries} queries.\n")
    return dict(table_groups)


# Step 2: Frequency analysis with Jaccard merging
def calculate_column_frequencies(grouped_queries: dict, modifiers: list, threshold_ratio: float, jaccard_threshold: float = 0.8) -> list[dict]:

    def stringify_key(key):
        if isinstance(key, (tuple, list)):
            return ", ".join(stringify_key(k) for k in key)
        return str(key)

    print("📈 Calculating query shape frequencies with Jaccard merging...\n")

    global total_queries
    min_queries = total_queries * threshold_ratio
    group_id = 0

    summarized_groups = []
    for table_key, queries in tqdm(grouped_queries.items(), desc="🔍 Processing groups"):
        # All the group must have the minimum queries, so that a part of it can have it
        if len(queries) > min_queries:

            # Since we do not have information on the schema of the tables, we assume '*' is simply the union of all attributes appearing in the queries
            all_columns = set()
            pattern_counts = defaultdict(int)
            for query in queries:
                columns = query.get("project", [])
                assert columns, f"Some query does not have projected columns: {query}"
                column_key = tuple(sorted(set(columns)))
                pattern_counts[column_key] += 1
                if columns != ['*']:
                    all_columns |= set(columns)

            merged_patterns = []
            used = set()
            patterns = list(pattern_counts.items())

            for i in range(len(patterns)):
                if i not in used:
                    shape_i, count_i = patterns[i]
                    if shape_i == ('*',):
                        star_found = True
                        set_i = all_columns
                    else:
                        star_found = False
                        set_i = set(shape_i)
                    merged = set_i.copy()
                    total_count = count_i

                    for j in range(i + 1, len(patterns)):
                        if j not in used:
                            shape_j, count_j = patterns[j]
                            if shape_j == ('*',):
                                star_found = True
                                set_j = all_columns
                            else:
                                set_j = set(shape_j)
                            jaccard = len(set_i & set_j) / len(set_i | set_j)
                            if jaccard >= jaccard_threshold:
                                print("Merging:", set_i, set_j, jaccard)
                                used.add(j)
                                merged |= set_j
                                total_count += count_j

                    if total_count > min_queries:
                        group_id += 1

                        try:
                            dedup_pattern = sorted(set(
                                t.split()[0] for t in queries[i].get("pattern", []) if "(" not in t
                            ))
                            if dedup_pattern != queries[i].get("pattern", []):
                                print("---------------------")
                                print("Original pattern: ", queries[i].get("pattern", []))
                                print("Cleaned pattern: ", dedup_pattern)
                                print("---------------------")

                            group_summary = {
                                "group_id": group_id,
                                "frequency": total_count / total_queries,
                                "original_query": queries[i].get("original_query", ""),
                                "pattern": dedup_pattern,
                                "project": list(merged) if not star_found else ['*'],
                                "filter": queries[i].get("filter"),
                                "has_nested_queries": queries[i].get("has_nested_queries"),
                            }
                            modifier_map = {
                                "distinct": "has_distinct",
                                "top": "top_value",
                                "orderby": "has_order_by",
                                "groupby": "has_group_by",
                            }

                            # Add only the modifiers used in grouping
                            for mod in modifiers:
                                json_key = modifier_map.get(mod)
                                if json_key in queries[i]:
                                    group_summary[json_key] = queries[i][json_key]
                            print(group_summary)
                            summarized_groups.append(group_summary)

                        except Exception as e:
                            print(f"⚠️ Failed processing group {group_id}: {e}")

    print("\n✅ Frequency analysis completed.\n")
    return summarized_groups


# Step 3: Save summarized representative queries per group
def save_grouped_queries(summarized_groups: list[dict], output_path: str):
    print(f"💾 Saving summarized group representatives to {output_path}...")

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({"queries": summarized_groups}, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved: {output_path}\n")
    except Exception as e:
        print(f"🚨 Failed to save grouped queries: {e}")
