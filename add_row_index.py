import json
import csv
import os
from pathlib import Path

def create_link_to_row_mapping(data_folder):
    """Create mapping of link to row index from CSV files, organized by ticker"""
    ticker_link_map = {}
    
    for csv_file in Path(data_folder).glob("*.csv"):
        ticker = csv_file.stem.rsplit('_', 2)[0]  # Extract ticker from filename
        ticker_link_map[ticker] = {}
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                ticker_link_map[ticker][row['link']] = idx
    
    return ticker_link_map

def process_results(results_folder, data_folder, output_folder):
    """Process JSON results and convert to JSONL with row indices"""
    ticker_link_map = create_link_to_row_mapping(data_folder)
    output_path = Path(output_folder)
    output_path.mkdir(exist_ok=True)
    
    for json_file in Path(results_folder).glob("master_*_articles.json"):
        ticker = json_file.stem.replace('master_', '').replace('_articles', '')
        
        with open(json_file, 'r', encoding='utf-8') as f:
            articles = json.load(f)
        
        output_file = output_path / f"{json_file.stem}.jsonl"
        
        # Get the link map for this specific ticker
        link_map = ticker_link_map.get(ticker, {})
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for article in articles:
                article['row_index'] = link_map.get(article['link'], -1)
                f.write(json.dumps(article, ensure_ascii=False) + '\n')
        
        print(f"Processed {json_file.name} -> {output_file.name}")

if __name__ == "__main__":
    results_folder = "results"
    data_folder = "../InvestingScraper/data"
    output_folder = "results"
    
    process_results(results_folder, data_folder, output_folder)
