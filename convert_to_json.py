"""
Convert CSV files with potentially malformed content fields to JSON.
The CSV files have columns: title, link, source, time, type, content
The 'content' column comes from HTML and may contain any characters.
"""

import csv
import json
import os
import glob
import io


def read_csv_robust(filepath):
    """
    Read a CSV file robustly, handling encoding issues and malformed rows.
    Returns a list of dicts.
    """
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
    expected_fields = ['title', 'link', 'source', 'time', 'type', 'content']

    for encoding in encodings:
        try:
            records = []
            with open(filepath, 'r', encoding=encoding, errors='replace', newline='') as f:
                reader = csv.DictReader(f)

                # Validate header
                if reader.fieldnames:
                    actual_fields = [f.strip() for f in reader.fieldnames]
                    print(f"  Fields found: {actual_fields}")
                    if actual_fields != expected_fields:
                        print(f"  WARNING: Expected {expected_fields}, got {actual_fields}")

                for i, row in enumerate(reader):
                    # Clean up any extra whitespace in keys
                    clean_row = {k.strip(): v for k, v in row.items() if k is not None}
                    records.append(clean_row)

            print(f"  Read {len(records)} records using encoding: {encoding}")
            return records

        except Exception as e:
            print(f"  Attempt with encoding '{encoding}' failed: {e}")
            continue

    return None


def read_csv_fallback(filepath):
    """
    Fallback: manually parse the CSV by reading raw bytes and reconstructing records.
    Handles cases where the csv module fails due to severely malformed content.
    """
    print("  Trying fallback manual parser...")
    records = []
    expected_fields = ['title', 'link', 'source', 'time', 'type', 'content']

    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace', newline='') as f:
            content = f.read()

        # Use csv.reader on the full content
        reader = csv.reader(io.StringIO(content))
        header = None

        for row in reader:
            if header is None:
                header = [h.strip() for h in row]
                continue

            if not row:
                continue

            # Pad or truncate row to match header length
            while len(row) < len(header):
                row.append('')
            if len(row) > len(header):
                # Merge extra columns into the last field (content)
                row = row[:len(header) - 1] + [','.join(row[len(header) - 1:])]

            record = dict(zip(header, row))
            records.append(record)

        print(f"  Fallback read {len(records)} records")
        return records

    except Exception as e:
        print(f"  Fallback also failed: {e}")
        return None


def convert_csv_to_json(csv_path, output_path=None):
    """
    Convert a single CSV file to JSON.
    """
    if output_path is None:
        output_path = os.path.splitext(csv_path)[0] + '.json'

    print(f"\nProcessing: {os.path.basename(csv_path)}")

    # Try primary method
    records = read_csv_robust(csv_path)

    # Try fallback if primary failed
    if records is None:
        records = read_csv_fallback(csv_path)

    if records is None:
        print(f"  ERROR: Could not parse {csv_path}")
        return False

    # Write JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"  Saved {len(records)} records -> {os.path.basename(output_path)}")

    # Quick validation: show first record summary
    if records:
        first = records[0]
        print(f"  Sample record keys: {list(first.keys())}")
        print(f"  Sample title: {first.get('title', 'N/A')[:80]}")
        content_preview = first.get('content', '')[:60].replace('\n', ' ')
        print(f"  Sample content: {content_preview}...")

    return True


def convert_all_csvs(data_dir, output_dir=None):
    """
    Convert all CSV files in data_dir to JSON.
    """
    if not os.path.exists(data_dir):
        print(f"ERROR: Directory not found: {data_dir}")
        return

    if output_dir is None:
        output_dir = data_dir

    os.makedirs(output_dir, exist_ok=True)

    csv_files = sorted(glob.glob(os.path.join(data_dir, '*.csv')))

    if not csv_files:
        print(f"No CSV files found in: {data_dir}")
        return

    print(f"Found {len(csv_files)} CSV file(s) in: {data_dir}")
    print("=" * 60)

    success_count = 0
    for csv_path in csv_files:
        json_filename = os.path.splitext(os.path.basename(csv_path))[0] + '.json'
        json_path = os.path.join(output_dir, json_filename)

        if convert_csv_to_json(csv_path, json_path):
            success_count += 1

    print("\n" + "=" * 60)
    print(f"Done: {success_count}/{len(csv_files)} files converted successfully.")


if __name__ == "__main__":
    import sys

    # Default: look for data directory relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'results')

    # Allow overriding via command line argument
    if len(sys.argv) > 1:
        data_dir = sys.argv[1]

    # Optionally specify output directory as second argument
    output_dir = None
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]

    convert_all_csvs(data_dir, output_dir)
