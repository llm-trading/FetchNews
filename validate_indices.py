import json
import csv
from pathlib import Path
from collections import defaultdict

def validate_row_indices(results_folder, data_folder):
    """Validate that row indices in JSONL match the CSV files"""
    
    # Build CSV link to row mapping with ticker info
    csv_data = defaultdict(dict)
    
    for csv_file in Path(data_folder).glob("*.csv"):
        ticker = csv_file.stem.rsplit('_', 2)[0]  # Extract ticker from filename
        print(f"\nReading CSV: {csv_file.name}")
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                csv_data[ticker][row['link']] = {
                    'csv_index': idx,
                    'csv_file': csv_file.name,
                    'title': row['title']
                }
        
        print(f"  Found {len(csv_data[ticker])} rows")
    
    # Validate JSONL files
    total_errors = 0
    total_checked = 0
    
    for jsonl_file in Path(results_folder).glob("*.jsonl"):
        ticker = jsonl_file.stem.replace('master_', '').replace('_articles', '')
        print(f"\n{'='*80}")
        print(f"Validating: {jsonl_file.name}")
        print(f"Ticker: {ticker}")
        print(f"{'='*80}")
        
        if ticker not in csv_data:
            print(f"  WARNING: No CSV data found for ticker '{ticker}'")
            continue
        
        errors = []
        
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                article = json.loads(line)
                link = article['link']
                jsonl_index = article.get('row_index', -1)
                
                total_checked += 1
                
                if link in csv_data[ticker]:
                    expected_index = csv_data[ticker][link]['csv_index']
                    csv_file = csv_data[ticker][link]['csv_file']
                    
                    if jsonl_index != expected_index:
                        errors.append({
                            'line': line_num,
                            'title': article['title'][:60],
                            'link': link,
                            'jsonl_index': jsonl_index,
                            'expected_index': expected_index,
                            'csv_file': csv_file
                        })
                        total_errors += 1
                else:
                    # Check if link exists in other tickers
                    found_in = []
                    for other_ticker, links in csv_data.items():
                        if link in links:
                            found_in.append((other_ticker, links[link]['csv_index'], links[link]['csv_file']))
                    
                    if found_in:
                        errors.append({
                            'line': line_num,
                            'title': article['title'][:60],
                            'link': link,
                            'jsonl_index': jsonl_index,
                            'expected_index': found_in[0][1],
                            'csv_file': found_in[0][2],
                            'note': f'Link found in {found_in[0][0]} instead of {ticker}'
                        })
                        total_errors += 1
                    else:
                        errors.append({
                            'line': line_num,
                            'title': article['title'][:60],
                            'link': link,
                            'jsonl_index': jsonl_index,
                            'expected_index': 'NOT FOUND',
                            'csv_file': 'NONE',
                            'note': 'Link not found in any CSV'
                        })
                        total_errors += 1
        
        if errors:
            print(f"\n  [X] Found {len(errors)} mismatches:")
            for err in errors[:10]:  # Show first 10 errors
                print(f"\n  Line {err['line']}:")
                print(f"    Title: {err['title']}...")
                print(f"    JSONL index: {err['jsonl_index']}")
                print(f"    Expected index: {err['expected_index']}")
                print(f"    CSV file: {err['csv_file']}")
                if 'note' in err:
                    print(f"    Note: {err['note']}")
            
            if len(errors) > 10:
                print(f"\n  ... and {len(errors) - 10} more errors")
        else:
            print(f"\n  [OK] All indices match correctly!")
    
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total articles checked: {total_checked}")
    print(f"Total errors found: {total_errors}")
    print(f"Accuracy: {((total_checked - total_errors) / total_checked * 100):.2f}%" if total_checked > 0 else "N/A")

if __name__ == "__main__":
    results_folder = "results"
    data_folder = "../InvestingScraper/data"
    
    validate_row_indices(results_folder, data_folder)
