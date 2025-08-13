"""
sql_optimize_pipeline.py

Sequential pipeline:
1. Read all .sql files in SQL_FOLDER
2. Split into whole queries (query-aware splitter)
3. Normalize and parameterize queries
4. Deduplicate by normalized-hash (exact canonical duplicates)
5. For each unique query: call LLM (gpt-4o-mini) sequentially to extract entities/attributes/query_type and optimization suggestions
6. Aggregate and save JSON + suggested reusable SQL library

Requires: pip install openai
Optional: pip install sqlparse  (for nicer SQL canonicalization)
Set OPENAI_API_KEY in your environment.
"""

import os
from dotenv import load_dotenv
import re
import glob
import json
import hashlib
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

import sqlparse
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

def simple_parameterize(sql):
    """
    Replace quoted strings and numeric literals with placeholders to canonicalize.
    Returns param_sql and a mapping of parameter values (for trace if needed).
    """
    params = []
    def repl_str(m):
        params.append(m.group(0))
        return ":param" + str(len(params))
    # quoted strings '...' or "..."
    sql = re.sub(r"('(?:\\'|[^'])*')", repl_str, sql)
    sql = re.sub(r'("(?:\\"|[^"])*")', repl_str, sql)
    # numeric literals (integers, decimals) - beware of identifiers; we only replace bare numbers
    sql = re.sub(r"(?<![\w\):])(-?\d+\.\d+|-?\d+)(?![\w\(.:])", repl_str, sql)
    return sql, params

SQL_KEYWORDS = set([
    "SELECT","FROM","WHERE","JOIN","INNER","LEFT","RIGHT","FULL","ON","GROUP","BY",
    "ORDER","LIMIT","OFFSET","HAVING","UNION","ALL","DISTINCT","INSERT","INTO",
    "VALUES","UPDATE","SET","DELETE","CREATE","WITH","AS","CASE","WHEN","THEN","END"
])

def basic_normalize(sql):
    """
    Basic normalization: uppercase keywords, collapse whitespace.
    If sqlparse is available, use it to format then collapse whitespace.
    """
    s = sql.strip()
    if sqlparse:
        try:
            s = sqlparse.format(s, keyword_case='upper', reindent=False, strip_comments=False)
        except Exception:
            pass
    else:
        # naive uppercase keywords
        def upkw(m):
            word = m.group(0)
            if word.upper() in SQL_KEYWORDS:
                return word.upper()
            return word
        s = re.sub(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", upkw, s)
    # collapse whitespace to single space
    s = re.sub(r"\s+", " ", s).strip()
    return s

def canonicalize_query(sql):
    """
    Parameterize + normalize -> canonical string used for hashing/dedup detection
    """
    param_sql, params = simple_parameterize(sql)
    norm = basic_normalize(param_sql)
    return norm

def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

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
  "entities": ["table1", "schema.table2", ...],         // list of real table names or CTE names used (do not list aliases)
  "attributes": ["col1", "col2", ...],                 // column names referenced (best-effort)
  "query_type": "SELECT|INSERT|UPDATE|DELETE|CREATE|OTHER",
  "optimization": {{
     "summary": "short natural-language suggestion(s) for optimization",
     "rewritten_query": "OPTIONAL: a recommended improved query text or parameterized template (if safe to rewrite). If none, return null",
     "index_suggestions": ["table.col", ...]           // optional list of index suggestions
  }}
}}

Be conservative: do not invent tables or columns that are not present. If you are unsure, state best-effort but keep to what's in the query text.
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
                # if couldn't parse, wrap raw text in a minimal JSON fallback
                parsed = {
                    "entities": [],
                    "attributes": [],
                    "query_type": "OTHER",
                    "optimization": {"summary": "llm returned unparsable json", "rewritten_query": None, "index_suggestions": []}
                }
            return parsed
        except Exception as e:
            print(f"LLM error (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(retry_delay * (1 + attempt))
    # after retries, return default
    return {
        "entities": [],
        "attributes": [],
        "query_type": "OTHER",
        "optimization": {"summary": "failed to call LLM", "rewritten_query": None, "index_suggestions": []}
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

    # Step B: Normalize/canonicalize & dedupe exact normalized
    normalized_map = {}  # canonical -> {hash, canonical_text, occurrences: [ (file, qid) ], raw_examples:[raw_sqls]}
    for inst in all_query_instances:
        raw = inst["sql"]
        canonical = canonicalize_query(raw)
        key_hash = hash_text(canonical)
        if key_hash not in normalized_map:
            normalized_map[key_hash] = {
                "canonical": canonical,
                "occurrences": [],
                "raw_examples": []
            }
        normalized_map[key_hash]["occurrences"].append({"file": inst["file"], "query_id": inst["query_id"]})
        normalized_map[key_hash]["raw_examples"].append(raw)

    print(f"Found {len(normalized_map)} unique canonical queries after parameterization & normalization (exact dedupe).")

    # Save unique queries file
    unique_list = []
    for k, v in normalized_map.items():
        unique_list.append({
            "id": k,
            "canonical": v["canonical"],
            "occurrences": v["occurrences"],
            "raw_example": v["raw_examples"][0]
        })
    # Removed writing unique_queries.json as requested

    # Step C: Sequentially call LLM on each unique canonical query to extract info + optimization suggestions
    optimization_results = {}
    total = len(unique_list)
    for i, entry in enumerate(unique_list, start=1):
        qid = entry["id"]
        raw_example = entry["raw_example"]
        print(f"[{i}/{total}] Analyzing unique query {qid} (occurrences: {len(entry['occurrences'])}) ...")
        llm_out = analyze_with_llm(raw_example)
        # keep what we need
        optimization_results[qid] = {
            "canonical": entry["canonical"],
            "occurrences": entry["occurrences"],
            "analysis": llm_out
        }
        # safe small sleep to avoid bursts (you can tune/remove)
        time.sleep(0.3)

    # Step D: Aggregate per-file results using the per-query analyses
    per_file_results = {}
    for path in file_paths:
        per_file_results[path] = {"entities": set(), "attributes": set(), "query_types": set(), "queries": []}

    for qhash, data in optimization_results.items():
        analysis = data["analysis"]
        occs = data["occurrences"]
        for occ in occs:
            fp = occ["file"]
            per_file_results[fp]["entities"].update(analysis.get("entities", []))
            per_file_results[fp]["attributes"].update(analysis.get("attributes", []))
            per_file_results[fp]["query_types"].add(analysis.get("query_type", "OTHER"))
            per_file_results[fp]["queries"].append({
                "unique_id": qhash,
                "canonical": data["canonical"],
                "analysis": analysis
            })

    # Convert sets to lists
    for fp, v in per_file_results.items():
        v["entities"] = sorted(list(v["entities"]))
        v["attributes"] = sorted(list(v["attributes"]))
        v["query_types"] = sorted(list(v["query_types"]))

    # Step E: Detect duplicates map and build reusable library from LLM rewritten_query where given
    duplicate_map = {}
    for qid, data in optimization_results.items():
        if len(data["occurrences"]) > 1:
            query_text = data.get("canonical", "")
            duplicate_map[query_text] = data["occurrences"]
    reusable_queries = []
    for qid, data in optimization_results.items():
        opt = data["analysis"].get("optimization", {})
        rewritten = opt.get("rewritten_query")
        if rewritten and rewritten.strip().lower() != "null":
            reusable_queries.append({
                "id": qid,
                "rewritten_query": rewritten,
                "summary": opt.get("summary", "")
            })

    # Save final outputs
    with open(os.path.join(OUTPUT_DIR, "per_file_results.json"), "w", encoding="utf-8") as f:
        json.dump(per_file_results, f, indent=2)
    with open(os.path.join(OUTPUT_DIR, "duplicate_map.json"), "w", encoding="utf-8") as f:
        json.dump(duplicate_map, f, indent=2)

    if reusable_queries:
        with open(os.path.join(OUTPUT_DIR, "reusable_library.sql"), "w", encoding="utf-8") as f:
            f.write("-- Reusable suggested queries (generated by LLM)\n\n")
            for r in reusable_queries:
                f.write(f"-- id: {r['id']}\n-- summary: {r['summary']}\n")
                f.write(r['rewritten_query'].strip() + "\n\n")

    print("✅ Pipeline complete. Outputs saved to:", OUTPUT_DIR)
    print(f"Unique queries: {len(unique_list)}, Reusable suggestions: {len(reusable_queries)}")
    print(f"Total time taken: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
