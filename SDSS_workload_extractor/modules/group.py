import json
from collections import defaultdict
from tqdm import tqdm


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

    for query in tqdm(queries, desc="📊 Building groups"):
        tables = query.get("pattern", [])
        table_set = tuple(sorted(set(
            t.lower().split()[0] for t in tables if "(" not in t
        )))
        if not table_set:
            continue

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

    for i, (_, group_queries) in enumerate(table_groups.items()):
        freq = len(group_queries)
        for q in group_queries:
            q["group_id"] = i
            q["frequency"] = freq

    print(f"✅ Created {len(table_groups)} groups.\n")
    return dict(table_groups)


# Step 2: Frequency analysis with Jaccard merging
def calculate_column_frequencies(grouped_queries: dict, threshold_ratio: float, jaccard_threshold: float = 0.8) -> dict:
    print("📈 Calculating query shape frequencies with Jaccard merging...\n")
    result = {}

    def stringify_key(key):
        if isinstance(key, (tuple, list)):
            return ", ".join(stringify_key(k) for k in key)
        return str(key)

    for table_key, queries in tqdm(grouped_queries.items(), desc="🔍 Processing groups"):
        total = len(queries)
        min_count = int(total * threshold_ratio)

        pattern_counts = defaultdict(int)
        for query in queries:
            columns = query.get("project", [])
            if not columns:
                continue
            column_key = tuple(sorted(set(columns)))
            pattern_counts[column_key] += 1

        merged_patterns = []
        used = set()
        patterns = list(pattern_counts.items())

        for i in range(len(patterns)):
            if i in used:
                continue
            shape_i, count_i = patterns[i]
            set_i = set(shape_i)
            merged = set_i.copy()
            total_count = count_i

            for j in range(i + 1, len(patterns)):
                if j in used:
                    continue
                shape_j, count_j = patterns[j]
                set_j = set(shape_j)
                jaccard = len(set_i & set_j) / len(set_i | set_j)
                if jaccard >= jaccard_threshold:
                    used.add(j)
                    merged |= set_j
                    total_count += count_j

            merged_patterns.append((tuple(sorted(merged)), total_count))

        filtered = {
            ", ".join(cols): count
            for cols, count in merged_patterns
            if count >= min_count
        }

        result[stringify_key(table_key)] = filtered

    print("\n✅ Frequency analysis completed.\n")
    return result


# Step 3: Save summarized representative queries per group
def save_grouped_queries(grouped: dict, output_path: str, modifiers: list):
    print(f"💾 Saving summarized group representatives to {output_path}...")
    summarized_groups = []

    for group_id, group_queries in enumerate(grouped.values()):
        if not group_queries:
            continue

        first_query = group_queries[0]

        try:
            dedup_pattern = sorted(set(
                t.split()[0] for t in first_query.get("pattern", []) if "(" not in t
            ))
            dedup_project = sorted(set(first_query.get("project", [])))

            group_summary = {
                "group_id": group_id,
                "frequency": len(group_queries),
                "original_query": first_query.get("original_query", ""),
                "pattern": dedup_pattern,
                "project": dedup_project,
                "filter": first_query.get("filter"),
                "has_nested_queries": first_query.get("has_nested_queries"),
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
                if json_key in first_query:
                    group_summary[json_key] = first_query[json_key]
            
            summarized_groups.append(group_summary)

        except Exception as e:
            print(f"⚠️ Failed processing group {group_id}: {e}")

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({"queries": summarized_groups}, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved: {output_path}\n")
    except Exception as e:
        print(f"🚨 Failed to save grouped queries: {e}")


# Step 4: Save frequency results
def save_frequencies(frequencies: dict, output_path: str):
    print(f"💾 Saving column frequency analysis to {output_path}...")
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(frequencies, f, indent=2, ensure_ascii=False)
        print(f"✅ Saved: {output_path}\n")
    except Exception as e:
        print(f"🚨 Failed to save frequency file: {e}")
