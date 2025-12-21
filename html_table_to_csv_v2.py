#!/usr/bin/env python3

import sys
import os
import pandas as pd

HTML_EXTS = (".html", ".htm")
APP_REF_NAME = "Application Reference"
ACTION_COL_NAME = "Transaction Type"  # new third column in composite key

def find_html_files():
    return sorted(
        f for f in os.listdir(".")
        if f.lower().endswith(HTML_EXTS)
    )

def normalize_columns(df):
    """
    Ensure correct headers:
    - Use existing column headers if valid
    - Promote first row to headers if it contains Application Reference
    """
    cols = [str(c).strip() for c in df.columns]

    if APP_REF_NAME in cols and ACTION_COL_NAME in cols:
        df.columns = cols
        return df

    first_row = df.iloc[0].astype(str).str.strip().tolist()

    if APP_REF_NAME in first_row and ACTION_COL_NAME in first_row:
        df = df.copy()
        df.columns = first_row
        df = df.iloc[1:].reset_index(drop=True)
        return df

    return df

def find_date_column(df):
    for col in df.columns:
        if "date" in str(col).lower():
            return col
    return None

def load_table(input_file):
    try:
        tables = pd.read_html(input_file, header=0)
    except ValueError:
        print(f"[SKIP] No tables found in {input_file}")
        return None

    df = tables[0]
    df = normalize_columns(df)
    df["_source_file"] = input_file
    return df

def detect_discrepancies(df, date_col, ref_col, action_col):
    """
    Detect discrepancies ONLY when composite key appears across different files:
    (date_col, ref_col, action_col)
    """
    discrepancies = []

    grouped = df.groupby([date_col, ref_col, action_col], dropna=False)

    for key, group in grouped:
        source_files = group["_source_file"].unique()

        # Only compare across different files
        if len(source_files) <= 1:
            continue

        reference_file = source_files[0]
        reference_row = group[group["_source_file"] == reference_file].iloc[0]

        for src in source_files[1:]:
            compare_rows = group[group["_source_file"] == src]

            for _, row in compare_rows.iterrows():
                diffs = {}

                for col in df.columns:
                    if col in (date_col, ref_col, action_col, "_source_file"):
                        continue

                    a = reference_row[col]
                    b = row[col]

                    if pd.isna(a) and pd.isna(b):
                        continue
                    if a != b:
                        diffs[col] = (a, b)

                if diffs:
                    discrepancies.append({
                        "key": key,
                        "file_a": reference_row["_source_file"],
                        "file_b": row["_source_file"],
                        "differences": diffs
                    })

    return discrepancies

def convert_individual(files):
    for file in files:
        df = load_table(file)
        if df is None:
            continue

        date_col = find_date_column(df)
        if date_col:
            df[date_col] = pd.to_datetime(
                df[date_col],
                errors="coerce",
                dayfirst=True
            )
            df = df.sort_values(by=date_col)

        output = os.path.splitext(file)[0] + ".csv"
        df.drop(columns=["_source_file"], inplace=True)
        df.to_csv(output, index=False)

        msg = f"[OK] {file} → {output}"
        msg += f" (sorted by '{date_col}')" if date_col else " (no date column)"
        print(msg)

def merge_all(files):
    dfs = []

    for file in files:
        df = load_table(file)
        if df is not None:
            dfs.append(df)

    if not dfs:
        print("No valid tables to merge")
        sys.exit(1)

    common_cols = set.intersection(*(set(df.columns) for df in dfs))
    dfs = [df[list(common_cols)] for df in dfs]

    merged_df = pd.concat(dfs, ignore_index=True)

    # Ensure composite key columns exist
    for col in [APP_REF_NAME, ACTION_COL_NAME]:
        if col not in merged_df.columns:
            print(f"Error: '{col}' column not found")
            sys.exit(1)

    date_col = find_date_column(merged_df)
    if not date_col:
        print("Error: no column header containing 'date' found")
        sys.exit(1)

    merged_df[date_col] = pd.to_datetime(
        merged_df[date_col],
        errors="coerce",
        dayfirst=True
    )

    discrepancies = detect_discrepancies(
        merged_df,
        date_col=date_col,
        ref_col=APP_REF_NAME,
        action_col=ACTION_COL_NAME
    )

    # Deduplicate rows using 3-column composite key
    merged_df = merged_df.drop_duplicates(
        subset=[date_col, APP_REF_NAME, ACTION_COL_NAME],
        keep="first"
    )

    merged_df = merged_df.sort_values(by=date_col)
    merged_df.drop(columns=["_source_file"], inplace=True)

    output = "merged.csv"
    merged_df.to_csv(output, index=False)

    print(f"[MERGED] {len(merged_df)} unique transactions → {output}")

    if discrepancies:
        print("\n⚠️  DISCREPANCIES FOUND (cross-file only)")
        for d in discrepancies:
            print(f"\nKey: {d['key']}")
            print(f"  File A: {d['file_a']}")
            print(f"  File B: {d['file_b']}")
            for col, (a, b) in d["differences"].items():
                print(f"    {col}: '{a}' ≠ '{b}'")
        print(f"\nTotal discrepancies: {len(discrepancies)}")
    else:
        print("No discrepancies detected ✔")

def main():
    args = sys.argv[1:]

    merge_flag = "--merge-all" in args
    args = [a for a in args if a != "--merge-all"]

    if len(args) > 1:
        print("Usage: html_table_to_csv.py [file.html] [--merge-all]")
        sys.exit(1)

    if args:
        files = [args[0]]
        if not os.path.isfile(files[0]):
            print(f"Error: file not found: {files[0]}")
            sys.exit(1)
    else:
        files = find_html_files()
        if not files:
            print("No HTML files found in current directory")
            sys.exit(0)

    if merge_flag:
        merge_all(files)
    else:
        convert_individual(files)

if __name__ == "__main__":
    main()
