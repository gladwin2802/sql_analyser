"""
sql_analyzer.py

Sequential pipeline:
1. Read all .sql files in SQL_FOLDER
2. Split into whole queries (query-aware splitter)
3. For each query: call LLM (gpt-4o-mini) to extract entities/attributes and query summary
4. Aggregate and save JSON with per-file results

Setup:
1. Install dependencies: pip install -r requirements.txt
2. Set OPENAI_API_KEY in your environment or .env file
3. Place SQL files in ./sql_files directory
4. Run: python main.py

Output:
- per_file_results.json: Contains entity-attribute mappings and query summaries organized by file
"""

import os
from dotenv import load_dotenv
import re
import glob
import json
import time
from dotenv import load_dotenv

load_dotenv()
# --- configure ---
SQL_FOLDER = "./sql_files"
MODEL = os.getenv("OPENAI_MODEL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OUTPUT_DIR = "./sql_analysis_output"

load_dotenv()

# Safety checks
if OPENAI_API_KEY is None:
    raise RuntimeError("Set OPENAI_API_KEY environment variable before running.")

import openai

openai.api_key = OPENAI_API_KEY

# ---------------------------------------------------
# Utilities
# ---------------------------------------------------

def query_aware_split(sql_text):
    """
    Split on semicolons that terminate statements while avoiding semicolons
    inside single/double quotes or inside parentheses (basic state machine).
    Returns list of queries **without** the trailing semicolon.
    """
    queries = []
    buf = []
    in_single = False
    in_double = False
    paren_depth = 0

    i = 0
    while i < len(sql_text):
        ch = sql_text[i]
        buf.append(ch)

        # handle quote toggles (ignore escaped quotes)
        if ch == "'" and not in_double:
            # check if escaped by backslash (simple heuristic)
            if not (i > 0 and sql_text[i-1] == "\\"):
                in_single = not in_single
        elif ch == '"' and not in_single:
            if not (i > 0 and sql_text[i-1] == "\\"):
                in_double = not in_double
        elif ch == "(" and not in_single and not in_double:
            paren_depth += 1
        elif ch == ")" and not in_single and not in_double:
            paren_depth = max(0, paren_depth - 1)
        elif ch == ";" and not in_single and not in_double and paren_depth == 0:
            # statement terminator
            q = "".join(buf).strip()
            if q.endswith(";"):
                q = q[:-1].strip()
            if q:
                queries.append(q)
            buf = []

        i += 1

    # leftover
    leftover = "".join(buf).strip()
    if leftover:
        queries.append(leftover)
    # filter out only comments / empty
    queries = [q for q in queries if re.search(r"\w", q)]
    return queries

def safe_json_load(s):
    """
    Attempt to load JSON from GPT response even if it has trailing commentary.
    Looks for first '{' and last '}' and parse between them.
    """
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        # try to extract first JSON object
        start = s.find("{")
        end = s.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(s[start:end+1])
            except Exception:
                pass
    # fallback
    return None

# ---------------------------------------------------
# LLM prompts for per-query analysis
# ---------------------------------------------------

BASE_PROMPT = """
You are an SQL analysis assistant. For the provided SQL query, produce a JSON object EXACTLY in this format (and nothing else):

{{
  "entity_attributes": {{
    "table1": ["col1", "col2", ...],           // map table names to their columns used
    "schema.table2": ["col3", "col4", ...],    // include schema if present
    "cte_name": ["col5", "col6", ...]          // include CTE names if used
  }},
  "summary": "detailed explanation of what this query does, including any formulas, calculations, or business logic it implements"
}}

Be conservative: do not invent tables or columns that are not present. If you are unsure, state best-effort but keep to what's in the query text.
Map each table/CTE to the specific columns that are referenced from it in the query.
SQL_QUERY:
---
{sql}
---
Respond only with the JSON object (no extra commentary).
"""

def analyze_with_llm(query_text, model=MODEL, max_retries=3, retry_delay=1.0):
    prompt = BASE_PROMPT.format(sql=query_text)
    for attempt in range(max_retries):
        try:
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.choices[0].message.content
            parsed = safe_json_load(text)
            if parsed is None:
                parsed = {
                    "entity_attributes": {},
                    "summary": "llm returned unparsable json"
                }
            return parsed
        except Exception as e:
            print(f"LLM error (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(retry_delay * (1 + attempt))
    # after retries, return default
    return {
        "entity_attributes": {},
        "summary": "failed to call LLM"
    }

# ---------------------------------------------------
# Pipeline main
# ---------------------------------------------------

def ensure_out_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def main():
    ensure_out_dir()
    start_time = time.time()
    
    # Step A: Read files -> extract queries
    queries_by_file = {}
    all_query_instances = []  # (file, query_id, raw_sql)
    file_paths = sorted(glob.glob(os.path.join(SQL_FOLDER, "*.sql")))
    if not file_paths:
        print(f"No .sql files found in '{SQL_FOLDER}'. Put SQL files there and re-run.")
        return

    for path in file_paths:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        queries = query_aware_split(content)
        queries_by_file[path] = queries
        for idx, q in enumerate(queries, start=1):
            all_query_instances.append({"file": path, "query_id": idx, "sql": q})

    print(f"Discovered {len(all_query_instances)} total queries across {len(file_paths)} files.")

    # Step B: Initialize per-file results structure
    per_file_results = {}
    for path in file_paths:
        per_file_results[path] = {
            "entity_attributes": {}, 
            "queries": []
        }

    # Step C: Analyze each query and aggregate results
    total = len(all_query_instances)
    for i, inst in enumerate(all_query_instances, start=1):
        file_path = inst["file"]
        query_id = inst["query_id"]
        raw_sql = inst["sql"]
        
        print(f"[{i}/{total}] Analyzing query {query_id} from {os.path.basename(file_path)} ...")
        
        # Call LLM for analysis
        llm_out = analyze_with_llm(raw_sql)
        
        # Update per-file results - merge entity_attributes
        file_entity_attrs = per_file_results[file_path]["entity_attributes"]
        query_entity_attrs = llm_out.get("entity_attributes", {})
        
        for entity, attributes in query_entity_attrs.items():
            if entity not in file_entity_attrs:
                file_entity_attrs[entity] = set()
            file_entity_attrs[entity].update(attributes)
        
        per_file_results[file_path]["queries"].append({
            "query_id": query_id,
            "sql": raw_sql,
            "entity_attributes": query_entity_attrs,
            "summary": llm_out.get("summary", "")
        })
        
        # safe small sleep to avoid bursts
        time.sleep(0.2)

    # Step D: Convert sets to sorted lists for JSON serialization
    for fp, v in per_file_results.items():
        # Convert entity_attributes sets to sorted lists
        for entity, attributes in v["entity_attributes"].items():
            v["entity_attributes"][entity] = sorted(list(attributes))

    # Step E: Save final output
    with open(os.path.join(OUTPUT_DIR, "per_file_results.json"), "w", encoding="utf-8") as f:
        json.dump(per_file_results, f, indent=2)

    print("✅ Pipeline complete. Outputs saved to:", OUTPUT_DIR)
    print(f"Total queries analyzed: {len(all_query_instances)}")
    print(f"Total time taken: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
