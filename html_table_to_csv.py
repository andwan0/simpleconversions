#!/usr/bin/env python3

import sys
import os
import pandas as pd

def main():
    if len(sys.argv) != 2:
        print("Usage: html_table_to_csv.py <input.html>")
        sys.exit(1)

    input_file = sys.argv[1]

    if not os.path.isfile(input_file):
        print(f"Error: file not found: {input_file}")
        sys.exit(1)

    base, ext = os.path.splitext(input_file)
    if ext.lower() not in [".html", ".htm"]:
        print("Warning: input file does not have .html or .htm extension")

    output_file = base + ".csv"

    try:
        tables = pd.read_html(input_file)
    except ValueError:
        print("Error: no HTML tables found")
        sys.exit(1)

    df = tables[0]

    df.to_csv(output_file, index=False)

    print(f"Converted: {input_file} → {output_file}")
    print(f"Rows: {len(df)} | Columns: {len(df.columns)}")

if __name__ == "__main__":
    main()
