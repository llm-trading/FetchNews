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
    
    # Keywords to purge based on your requirements
    junk_keywords = [
        "InvestingPro", 
        "Should you be buying", 
        "ProPicks AI", 
        "Get real-time updates",
        "Advertisement"
    ]

    # Process in chunks of 50 to refresh browser
    for i in range(0, len(links_to_scrape), BROWSER_CYCLE):
        current_chunk = links_to_scrape[i : i + BROWSER_CYCLE]
        print(f"\n[Session] Fresh browser for {ticker}: Articles {i+1} to {i+len(current_chunk)}")
        
        try:
            # Added ad_block=True for speed and kept eager strategy
            with SB(uc=True, incognito=True, headless2=True, xvfb=True, ad_block=True,
                    agent=get_random_user_agent(), page_load_strategy="eager") as sb:
                
                sb.set_window_size(random.randint(1024, 1920), random.randint(768, 1080))
                
                for idx, row in enumerate(current_chunk):
                    # 1. Batch Break (Randomized jitter for better human simulation)
                    if idx > 0 and idx % COOLDOWN_BATCH == 0:
                        delay = random.uniform(15, 25)
                        print(f"  [zZz] Batch break: Cooling down for {delay:.1f}s...")
                        time.sleep(delay)

                    # 2. Check for PAID logic
                    if str(row['type']).lower() != 'free':
                        print(f"  > [{idx+1}] Skipping PAID: {row['title'][:30]}")
                        row['content'] = "PAID"
                        pd.DataFrame([row]).to_csv(master_path, mode='a', header=not os.path.exists(master_path), index=False)
                        scraped_in_this_session += 1
                        continue

                    print(f"  > [{idx+1}] Fetching FREE: {row['title'][:30]}...")
                    
                    try:
                        # 3. Enhanced Fetching Logic
                        sb.uc_open_with_reconnect(row['link'], 10)
                        
                        # Human jitter
                        sb.execute_script("window.scrollBy(0, 400);")
                        sb.sleep(1.2)

                        # 4. Target dynamic container using the successful test selector
                        container_selector = "div[class*='article_WYSIWYG'], div[class*='article_articlePage']"
                        
                        if not sb.is_element_visible(container_selector):
                            print(f"    [!] Container not found for: {row['link'][:40]}")
                            row['content'] = "SELECTOR_NOT_FOUND"
                        else:
                            # 5. Extract and Filter (p and h2 tags)
                            elements = sb.find_elements(f"{container_selector} p, {container_selector} h2")
                            
                            clean_content = []
                            for el in elements:
                                # Filter Stage A: Subscription Hook Parent Check
                                parent_html = el.get_attribute('outerHTML') or ""
                                if 'contextual-subscription-hook' in parent_html:
                                    continue
                                
                                # Filter Stage B: Junk Keywords & Length
                                text = el.text.strip()
                                if not text or len(text) < 10:
                                    continue
                                
                                if any(junk in text for junk in junk_keywords):
                                    continue
                                
                                clean_content.append(text)

                            final_text = "\n\n".join(clean_content)
                            row['content'] = final_text if final_text else "EMPTY_CONTENT"

                        # 6. Immediate Checkpoint
                        pd.DataFrame([row]).to_csv(master_path, mode='a', header=not os.path.exists(master_path), index=False)
                        scraped_in_this_session += 1
                        
                        # Post-scrape cooldown
                        sb.sleep(random.uniform(3, 6))

                    except Exception as e:
                        print(f"  [!] Fetch Error: {str(e)[:50]}")
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