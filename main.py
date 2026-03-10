import os
import random
import pandas as pd
import time
from seleniumbase import SB

# --- CONFIGURATION ---
SOURCE_REPO_URL = "https://github.com/llm-trading/InvestingScraper.git"
INPUT_DIR = "InvestingScraper/data"
RESULTS_DIR = "results"
TICKER_FILE = "tickers.txt"
RUN_LIMIT = 300           
BROWSER_CYCLE = 50        
COOLDOWN_BATCH = 10       
# ---------------------

def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    return random.choice(user_agents)

def scrape_batch(ticker, links_to_scrape, master_path):
    scraped_in_this_session = 0
    
    # Process in chunks of 50 to refresh browser
    for i in range(0, len(links_to_scrape), BROWSER_CYCLE):
        current_chunk = links_to_scrape[i : i + BROWSER_CYCLE]
        print(f"\n[Session] Fresh browser for {ticker}: Articles {i+1} to {i+len(current_chunk)}")
        
        try:
            with SB(uc=True, incognito=True, headless2=True, xvfb=True, 
                    agent=get_random_user_agent(), page_load_strategy="eager") as sb:
                
                sb.set_window_size(random.randint(1024, 1920), random.randint(768, 1080))
                
                for idx, row in enumerate(current_chunk):
                    # 1. Batch Break (Human-like pause every 10 articles)
                    if idx > 0 and idx % COOLDOWN_BATCH == 0:
                        print(f"  [zZz] Batch break: Cooling down...")
                        time.sleep(random.uniform(15, 25))

                    # 2. Check for PAID (Logic preserved to maintain row count)
                    if str(row['type']).lower() != 'free':
                        print(f"  > [{idx+1}] Skipping PAID: {row['title'][:30]}")
                        row['content'] = "PAID"
                        # Save even the PAID status to the master file immediately
                        pd.DataFrame([row]).to_csv(master_path, mode='a', header=not os.path.exists(master_path), index=False)
                        scraped_in_this_session += 1
                        continue

                    print(f"  > [{idx+1}] Fetching FREE: {row['title'][:30]}...")
                    
                    try:
                        sb.uc_open_with_reconnect(row['link'], 7)
                        
                        # --- HUMAN BEHAVIOR SIMULATION ---
                        sb.execute_script("window.scrollBy(0, 400);")
                        sb.sleep(1.5)
                        # ---------------------------------

                        sb.wait_for_element('div.articlePage', timeout=15)
                        paragraphs = sb.find_elements('div.articlePage p')
                        text = "\n\n".join([p.text.strip() for p in paragraphs if p.text.strip() and "Go deeper" not in p.text])
                        
                        row['content'] = text if text else "EMPTY_CONTENT"
                        pd.DataFrame([row]).to_csv(master_path, mode='a', header=not os.path.exists(master_path), index=False)
                        
                        scraped_in_this_session += 1
                        sb.sleep(random.uniform(4, 7))

                    except Exception as e:
                        print(f"  [!] Error: {str(e)[:50]}")
                        row['content'] = "ERROR_FETCHING"
                        pd.DataFrame([row]).to_csv(master_path, mode='a', header=not os.path.exists(master_path), index=False)
                        scraped_in_this_session += 1
        
        except Exception as e:
            print(f"[!!!] Browser Crash: {str(e)[:100]}. Restarting...")
            time.sleep(5)

    return scraped_in_this_session

if __name__ == "__main__":
    os.makedirs(RESULTS_DIR, exist_ok=True)
    if not os.path.exists("InvestingScraper"):
        os.system(f"git clone --depth 1 {SOURCE_REPO_URL}")

    with open(TICKER_FILE, "r") as f:
        tickers = [line.strip() for line in f if line.strip()]

    total_run_count = 0

    for t in tickers:
        if total_run_count >= RUN_LIMIT: break
        
        master_path = os.path.join(RESULTS_DIR, f"master_{t}_articles.csv")
        source_file = next((f for f in os.listdir(INPUT_DIR) if f.startswith(f"{t}_")), None)
        if not source_file: continue
        
        df_source = pd.read_csv(os.path.join(INPUT_DIR, source_file))
        
        # Check Progress
        done_links = set()
        if os.path.exists(master_path):
            # We use 'link' as the unique ID for the checkpoint
            done_links = set(pd.read_csv(master_path)['link'].tolist())

        # Filter links while maintaining source order
        pending = df_source[~df_source['link'].isin(done_links)]
        
        if pending.empty:
            print(f"[-] {t} is complete.")
            continue

        # Slice the workload for this run
        to_scrape = pending.head(RUN_LIMIT - total_run_count).to_dict('records')
        print(f"[*] {t}: Scraping {len(to_scrape)} articles to reach limit.")
        
        fetched = scrape_batch(t, to_scrape, master_path)
        total_run_count += fetched

    print(f"\n[FINISH] Total items processed this window: {total_run_count}")