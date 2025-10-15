#!/usr/bin/env python3

 

import argparse

import csv

import concurrent.futures

import io

import os

import sqlite3

import subprocess

import sys

import threading

from datetime import datetime, timezone

from typing import Any, Dict, Iterable, List, Optional, Tuple

 

DATE_OUTPUT_FMT = "%Y-%m-%d %H:%M:%S"

 

# ---------------------------------

# Utilities: datetime, bool, text

# ---------------------------------

 

def _try_parse_dt_with_patterns(s: str) -> Optional[datetime]:

    patterns = [

        "%Y-%m-%d %H:%M:%S%z",

        "%Y-%m-%d %H:%M:%S",

        "%Y-%m-%dT%H:%M:%S%z",

        "%Y-%m-%dT%H:%M:%S",

        "%Y-%m-%d",

        "%m/%d/%Y %H:%M:%S",

        "%m/%d/%Y",

        "%Y-%m-%d %H:%M:%S.%f%z",

        "%Y-%m-%d %H:%M:%S.%f",

        "%Y-%m-%dT%H:%M:%S.%f%z",

        "%Y-%m-%dT%H:%M:%S.%f",

    ]

    s2 = s.strip()

    if s2.endswith("Z"):

        s2 = s2[:-1] + "+0000"

    for p in patterns:

        try:

            return datetime.strptime(s2, p)

        except Exception:

            continue

    try:

        return datetime.fromisoformat(s.strip().replace("Z", "+00:00"))

    except Exception:

        return None

 

def normalize_datetime(value: Any) -> str:

    if value is None:

        return ""

    if isinstance(value, datetime):

        dt = value

    else:

        s = str(value).strip()

        if not s:

            return ""

        dt = _try_parse_dt_with_patterns(s)

        if dt is None:

            return s.replace("T", " ")

    if dt.tzinfo is not None:

        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    return dt.strftime(DATE_OUTPUT_FMT)

 

def normalize_bool_to_int(value: Any) -> int:

    if value is None:

        return 0

    if isinstance(value, (int,)):

        return 1 if value != 0 else 0

    s = str(value).strip().lower()

    if s in ("1", "true", "t", "y", "yes", "on"):

        return 1

    if s in ("0", "false", "f", "n", "no", "off", ""):

        return 0

    return 0

 

def truncate_with_suffix(base: str, suffix: str, max_len: int) -> str:

    if len(base) + len(suffix) <= max_len:

        return base + suffix

    return base[: max_len - len(suffix)].rstrip() + suffix

 

# ---------------------------------

# DB access helpers

# ---------------------------------

 

def _run_psql_copy(select_sql: str, host: str, port: int, dbname: str, user: str, password: Optional[str]) -> List[Dict[str, str]]:

    print(f"Running psql COPY for query: {select_sql.strip().splitlines()[0]} ...")

    copy_sql = f"COPY ({select_sql}) TO STDOUT WITH CSV HEADER"

    cmd = [

        "psql",

        "-h",

        host,

        "-p",

        str(port),

        "-U",

        user,

        "-d",

        dbname,

        "-c",

        copy_sql,

    ]

    env = os.environ.copy()

    if password:

        env["PGPASSWORD"] = password

    try:

        res = subprocess.run(cmd, env=env, capture_output=True, text=True, encoding="utf-8", check=False)

    except FileNotFoundError:

        raise RuntimeError("psql client not found. Please install PostgreSQL client tools or use SQLite mode.")

    if res.returncode != 0:

        raise RuntimeError(f"psql failed ({res.returncode}): {res.stderr.strip()}")

    data = res.stdout

    print(f"psql COPY finished, rows fetched: {data.count(chr(10)) - 1}")

    reader = csv.DictReader(io.StringIO(data))

    rows: List[Dict[str, str]] = [dict(r) for r in reader]

    return rows

 

def fetch_from_postgres(host: str, port: int, dbname: str, user: str, max_lists: Optional[int], password: Optional[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:

    print("Fetching data from PostgreSQL ...")

    lists_select = """

        SELECT

            id AS list_id,

            user_id AS borrower_id,

            REGEXP_REPLACE(name, '[,\\s]+', ' ', 'g') AS name,
            
            description,

            created_at AS date_created,

            modified_at AS date_updated,

            (share_token IS NOT NULL) AS public_list

        FROM lists

    """

    if max_lists is not None:

        lists_select += f"\nLIMIT {int(max_lists)}"

 

    items_select = """

        SELECT

            i.id AS item_id,

            i.record_id AS bib_id,

            i.created_at AS date_added,

            il.list_id AS list_id

        FROM items i

        LEFT JOIN item_lists il ON il.item_id = i.id

    """

    if max_lists is not None:

        items_select += f"\nLIMIT {int(max_lists)}"

 

    lists_rows = _run_psql_copy(lists_select, host, port, dbname, user, password)

    items_rows = _run_psql_copy(items_select, host, port, dbname, user, password)

    print(f"Fetched {len(lists_rows)} lists and {len(items_rows)} items from PostgreSQL.")

    return lists_rows, items_rows

 

def fetch_from_sqlite(sqlite_path: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:

    print(f"Fetching data from SQLite: {sqlite_path}")

    con = sqlite3.connect(sqlite_path)

    con.row_factory = sqlite3.Row

    try:

        cur = con.cursor()

        cur.execute(

            """

            SELECT

                id AS list_id,

                user_id AS borrower_id,

                name,

                description,

                created_at AS date_created,

                modified_at AS date_updated,

                CASE WHEN share_token IS NOT NULL THEN 1 ELSE 0 END AS public_list

            FROM lists

            """

        )

        lists_rows = [dict(r) for r in cur.fetchall()]

        cur.execute(

            """

            SELECT

                i.id AS item_id,

                i.record_id AS bib_id,

                i.created_at AS date_added,

                il.list_id AS list_id

            FROM items i

            LEFT JOIN item_lists il ON il.item_id = i.id

            """

        )

        items_rows = [dict(r) for r in cur.fetchall()]

        print(f"Fetched {len(lists_rows)} lists and {len(items_rows)} items from SQLite.")

        return lists_rows, items_rows

    finally:

        con.close()

 

# ---------------------------------

# Transformation logic

# ---------------------------------

 

def to_int_or_zero(v: Any) -> int:

    try:

        return int(str(v))

    except Exception:

        return 0

 

def chunked(seq: List[Any], size: int) -> Iterable[List[Any]]:

    for i in range(0, len(seq), size):

        yield seq[i:i + size]

 

def apply_field_limits(lists_out: List[Dict[str, Any]], name_max_len: int = 255, desc_max_len: int = 4000) -> None:

    for e in lists_out:

        name = (e.get("name") or "").strip()

        if len(name) > name_max_len:

            e["name"] = name[:name_max_len].rstrip()

        desc = e.get("description") or ""

        if len(desc) > desc_max_len:

            e["description"] = desc[:desc_max_len]

 

def ensure_unique_names(lists_out: List[Dict[str, Any]], name_max_len: int = 255) -> None:

    groups: Dict[str, List[int]] = {}

    for idx, e in enumerate(lists_out):

        base = (e.get("name") or "").strip()

        groups.setdefault(base, []).append(idx)

    for base, idxs in groups.items():

        if len(idxs) == 1:

            continue

        # Multiple: assign suffixes

        for i, idx in enumerate(sorted(idxs), start=1):

            suffix = f" ({i})"

            lists_out[idx]["name"] = truncate_with_suffix(base, suffix, name_max_len)

 

def normalize_list_row(r: Dict[str, Any]) -> Dict[str, Any]:

    return {

        "list_id": to_int_or_zero(r.get("list_id")),

        "borrower_id": to_int_or_zero(r.get("borrower_id")),

        "name": (r.get("name") or "").strip(),

        "description": r.get("description") or "",

        "date_created": normalize_datetime(r.get("date_created")),

        "date_updated": normalize_datetime(r.get("date_updated")),

        "public_list": normalize_bool_to_int(r.get("public_list")),

    }

 

def normalize_item_row(r: Dict[str, Any]) -> Dict[str, Any]:

    return {

        "list_id": to_int_or_zero(r.get("list_id")) if r.get("list_id") is not None else "",

        "bib_id": "" if r.get("bib_id") is None else str(r.get("bib_id")),

        "date_added": normalize_datetime(r.get("date_added")),

    }

 

def parallel_normalize_lists_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    with concurrent.futures.ProcessPoolExecutor() as executor:

        return list(executor.map(normalize_list_row, rows))

 

def parallel_normalize_item_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

    with concurrent.futures.ProcessPoolExecutor() as executor:

        return list(executor.map(normalize_item_row, rows))

 

def split_and_write_lists_and_items(lists_in: List[Dict[str, Any]],

                                   items_in: List[Dict[str, Any]],

                                   chunk_size: int,

                                   start_id: Optional[int],

                                   out_dir: str,

                                   max_lists: Optional[int] = None) -> int:

    # Apply max_lists limit if set

    if max_lists is not None:

        orig_count = len(lists_in)

        lists_in = lists_in[:max_lists]

        print(f"Limiting output to first {max_lists} lists (from {orig_count}) for debugging/testing.")

 

    # Group lists by user

    lists_by_user: Dict[int, List[Dict[str, Any]]] = {}

    for lst in lists_in:

        borrower_id = lst.get("borrower_id")

        if borrower_id is not None:

            lists_by_user.setdefault(borrower_id, []).append(lst)

        else:

            print(f"Warning: list_id {lst.get('list_id')} has no borrower_id and will be skipped.")

 

    # Group items by list_id

    items_by_list: Dict[int, List[Dict[str, Any]]] = {}

    items_no_list_by_user: Dict[int, List[Dict[str, Any]]] = {}

    for it in items_in:

        lid = it.get("list_id")

        if lid == "" or lid is None or lid == 0:

            borrower_id = it.get("borrower_id")

            if borrower_id is not None:

                items_no_list_by_user.setdefault(borrower_id, []).append(it)

            else:

                print(f"Warning: item_id {it.get('item_id')} with no list has no borrower_id and will be skipped.")

        else:

            items_by_list.setdefault(lid, []).append(it)

 

    # Track new list_ids for split lists

    existing_ids = [e["list_id"] for e in lists_in if isinstance(e.get("list_id"), int)]

    max_id = max(existing_ids) if existing_ids else 0

    next_id = start_id if start_id is not None else (max_id + 1 if max_id else 1_000_000)

 

    os.makedirs(out_dir, exist_ok=True)

    total_chunks = 0

    user_count = 0

    for borrower_id, user_lists in lists_by_user.items():

        user_count += 1

        print(f"Processing user {borrower_id} with {len(user_lists)} lists ...")

        user_dir = os.path.join(out_dir, str(borrower_id))

        os.makedirs(user_dir, exist_ok=True)

        lists_out = []

        new_lists_out = []

        items_out = []

        list_name_counts = {}

        for base in user_lists:

            lid = base["list_id"]

            assoc = items_by_list.get(lid, [])

            assoc.sort(key=lambda x: x.get("date_added") or "")

            if len(assoc) <= chunk_size:

                lists_out.append(dict(base))

                items_out.append((base["list_id"], assoc))

            else:

                chunked_items = list(chunked(assoc, chunk_size))

                for cidx, chunk in enumerate(chunked_items, start=1):

                    # Create new list for each chunk

                    new_lid = next_id

                    next_id += 1

                    # Append number to name for each chunk

                    base_name = base["name"]

                    name_count = list_name_counts.get(base_name, 1)

                    new_name = f"{base_name} ({name_count})"

                    list_name_counts[base_name] = name_count + 1

                    new_base = dict(base)

                    new_base["list_id"] = new_lid

                    new_base["name"] = new_name

                    new_lists_out.append(new_base)

                    items_out.append((new_lid, chunk))

        if not lists_out and not new_lists_out:

            print(f"Warning: No lists to write for user {borrower_id}.")

            continue

        # Write lists_1.csv (all original lists for user)

        lists_csv = os.path.join(user_dir, "lists_1.csv")

        with open(lists_csv, "w", newline="", encoding="utf-8") as f:

            w = csv.writer(f)

            w.writerow(["list_id", "borrower_id", "name", "description", "date_created", "date_updated", "public_list"])

            for e in lists_out:

                w.writerow([

                    e.get("list_id") or "",

                    e.get("borrower_id") or "",

                    e.get("name") or "",

                    e.get("description") or "",

                    e.get("date_created") or "",

                    e.get("date_updated") or "",

                    1 if e.get("public_list") == 1 else 0,

                ])

        print(f"  Wrote {len(lists_out)} lists to {lists_csv}")

        # Write new-lists_1.csv (all chunked lists for user)

        new_lists_csv = os.path.join(user_dir, "new-lists_1.csv")

        with open(new_lists_csv, "w", newline="", encoding="utf-8") as f:

            w = csv.writer(f)

            w.writerow(["list_id", "borrower_id", "name", "description", "date_created", "date_updated", "public_list"])

            for e in new_lists_out:

                w.writerow([

                    e.get("list_id") or "",

                    e.get("borrower_id") or "",

                    e.get("name") or "",

                    e.get("description") or "",

                    e.get("date_created") or "",

                    e.get("date_updated") or "",

                    1 if e.get("public_list") == 1 else 0,

                ])

        print(f"  Wrote {len(new_lists_out)} chunked lists to {new_lists_csv}")

        # Write list_items files for each list

        for idx, (lid, items_chunk) in enumerate(items_out, start=1):

            items_csv = os.path.join(user_dir, f"list_items_{lid}.csv")

            with open(items_csv, "w", newline="", encoding="utf-8") as f:

                w = csv.writer(f)

                w.writerow(["list_id", "bib_id", "date_added"])

                for it in items_chunk:

                    w.writerow([

                        it.get("list_id") or "",

                        it.get("bib_id") or "",

                        it.get("date_added") or "",

                    ])

            print(f"    Wrote {len(items_chunk)} items to {items_csv}")

        # Write no_list items for this user

        no_list_items = items_no_list_by_user.get(borrower_id, [])

        if no_list_items:

            chunked_no_list = list(chunked(no_list_items, chunk_size))

            for idx, chunk in enumerate(chunked_no_list, start=1):

                no_list_csv = os.path.join(user_dir, f"no_lists_{idx}.csv")

                with open(no_list_csv, "w", newline="", encoding="utf-8") as f:

                    w = csv.writer(f)

                    w.writerow(["list_id", "bib_id", "date_added"])

                    for it in chunk:

                        w.writerow([

                            it.get("list_id") or "",

                            it.get("bib_id") or "",

                            it.get("date_added") or "",

                        ])

                print(f"    Wrote {len(chunk)} items to {no_list_csv}")

            total_chunks += len(chunked_no_list)

        total_chunks += len(items_out)

        print(f"    Wrote user {borrower_id}: {lists_csv}, {new_lists_csv}, {len(items_out)} list_items files")

    print(f"  Split and write complete: {total_chunks} chunks written for {user_count} users.")

    return total_chunks

 

def main():

    print("Starting CSV generation script ...")

    parser = argparse.ArgumentParser(description="Generate lists.csv and list_items.csv from a database.")

    parser.add_argument("--dbtype", choices=["postgres", "sqlite"], default="postgres", help="Database type (default: postgres)")

    # PostgreSQL options

    parser.add_argument("--host", default=os.environ.get("PGHOST", "localhost"), help="PostgreSQL host")

    parser.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "5432")), help="PostgreSQL port")

    parser.add_argument("--dbname", default=os.environ.get("PGDATABASE"), help="PostgreSQL database name")

    parser.add_argument("--user", default=os.environ.get("PGUSER"), help="PostgreSQL user")

    parser.add_argument("--password", default=os.environ.get("PGPASSWORD"), help="PostgreSQL password")

    # SQLite option

    parser.add_argument("--sqlite-path", help="Path to SQLite database file")

    # Common

    parser.add_argument("--out-dir", required=True, help="Output directory for CSV files")

    parser.add_argument("--chunk-size", type=int, default=5000, help="Maximum items per list (default: 5000)")

    parser.add_argument("--start-id", type=int, help="Starting ID for new lists created when splitting (default: max existing + 1 or 1_000_000)")

    parser.add_argument("--max-lists", type=int, default=None, help="Limit the number of lists to output (for debugging/testing)")

    args = parser.parse_args()

    abs_out_dir = os.path.abspath(args.out_dir)

    print(f"Output directory: {abs_out_dir}")

    print(f"Arguments parsed: {args}")

 

    # Fetch data

    if args.dbtype == "postgres":

        missing = [k for k, v in {"dbname": args.dbname, "user": args.user}.items() if not v]

        if missing:

            print(f"Missing PostgreSQL parameters: {', '.join(missing)}", file=sys.stderr)

            sys.exit(2)

        print("Fetching data from PostgreSQL ...")

        lists_raw, items_raw = fetch_from_postgres(args.host, args.port, args.dbname, args.user, args.max_lists, args.password)

    else:

        if not args.sqlite_path:

            print("Please provide --sqlite-path for SQLite mode.", file=sys.stderr)

            sys.exit(2)

        if not os.path.exists(args.sqlite_path):

            print(f"SQLite file not found: {args.sqlite_path}", file=sys.stderr)

            sys.exit(2)

        print("Fetching data from SQLite ...")

        lists_raw, items_raw = fetch_from_sqlite(args.sqlite_path)

 

    print(f"Fetched {len(lists_raw)} lists and {len(items_raw)} items from DB.")

    if lists_raw:

        print(f"First list row: {lists_raw[0]}")

    if items_raw:

        print(f"First item row: {items_raw[0]}")

    if not lists_raw:

        print("No lists found in database. Exiting.")

        sys.exit(1)

    if not items_raw:

        print("No items found in database. Exiting.")

        sys.exit(1)

 

    print(f"Normalizing {len(lists_raw)} lists and {len(items_raw)} items ...")

    try:

        with concurrent.futures.ProcessPoolExecutor() as executor:

            fut_lists = executor.submit(parallel_normalize_lists_rows, lists_raw)

            fut_items = executor.submit(parallel_normalize_item_rows, items_raw)

            lists_norm = fut_lists.result()

            items_norm = fut_items.result()

    except Exception as e:

        print(f"Exception during normalization: {e}", file=sys.stderr)

        import traceback; traceback.print_exc()

        sys.exit(1)

    print(f"Normalization complete. Lists: {len(lists_norm)}, Items: {len(items_norm)}")

    if lists_norm:

        print(f"First normalized list: {lists_norm[0]}")

    if items_norm:

        print(f"First normalized item: {items_norm[0]}")

 

    # Apply max-lists limit if set

    if args.max_lists is not None:

        orig_count = len(lists_norm)

        lists_norm = lists_norm[:args.max_lists]

        print(f"Limiting output to first {args.max_lists} lists (from {orig_count}) for debugging/testing.")

    if not lists_norm:

        print("No lists to process after applying max-lists. Exiting.")

        sys.exit(1)

 

    print(f"Splitting lists and items with chunk size {args.chunk_size} ...")

    total_chunks = split_and_write_lists_and_items(lists_norm, items_norm, args.chunk_size, args.start_id, abs_out_dir, args.max_lists)

    print(f"Splitting complete. Output chunks: {total_chunks}")

    # Check output directory contents

    print(f"Output directory contents after run:")

    for root, dirs, files in os.walk(abs_out_dir):

        for name in files:

            print(f"  {os.path.join(root, name)}")

    print("Done.")

 

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        print("\nScript interrupted by user (Ctrl+C). Exiting.")

        sys.exit(130)

