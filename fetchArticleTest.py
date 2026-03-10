from seleniumbase import SB
import re

def test_article_extraction():
    # Target URL with the ads and subscription hooks
    test_url = "https://www.investing.com/news/stock-market-news/nvidia-taps-samsung-sk-hynix-as-exclusive-hbm4-suppliers-for-vera-rubin--report-4548666"
    
    print(f"[*] Starting optimized fetch (Ads Blocked): {test_url}")

    # ad_block=True is the key to speeding up Investing.com
    with SB(uc=True, headless=False, ad_block=True, incognito=True) as sb:
        try:
            # Open with a 10s timeout; ad_block makes this much faster
            sb.uc_open_with_reconnect(test_url, 10)
            
            # 1. Human jitter (Simulate a quick scroll)
            sb.execute_script("window.scrollBy(0, 400);")
            sb.sleep(1.2)

            # 2. Target the main content container
            # This matches the dynamic classes from your HTML examples
            container_selector = "div[class*='article_WYSIWYG'], div[class*='article_articlePage']"
            
            if not sb.is_element_visible(container_selector):
                print("[!] Container not found. Site may have blocked the IP or layout changed.")
                sb.save_screenshot("test_fail.png")
                return

            # 3. Extract all potential text elements (paragraphs and headers)
            elements = sb.find_elements(f"{container_selector} p, {container_selector} h2")
            
            clean_content = []
            
            # Keywords to purge based on your examples
            junk_keywords = [
                "InvestingPro", 
                "Should you be buying", 
                "ProPicks AI", 
                "Get real-time updates",
                "Advertisement"
            ]

            for el in elements:
                # Check if the element is part of the subscription hook div
                parent_html = el.get_attribute('outerHTML') or ""
                if 'contextual-subscription-hook' in parent_html:
                    continue
                
                text = el.text.strip()
                
                # Filter Logic
                if not text or len(text) < 10:
                    continue
                
                # Check against our list of junk phrases
                if any(junk in text for junk in junk_keywords):
                    continue
                
                clean_content.append(text)

            # 4. Results
            if clean_content:
                print(f"\n[SUCCESS] Extracted {len(clean_content)} clean paragraphs.")
                print("="*50)
                # Print the whole thing to verify no junk remains
                full_output = "\n\n".join(clean_content)
                print(full_output)
                print("="*50)
            else:
                print("[!] Extraction yielded no text after filtering.")

        except Exception as e:
            print(f"[CRITICAL ERROR] {e}")

if __name__ == "__main__":
    import time
    tick = time.time()
    test_article_extraction()
    tock = time.time()
    print(tock-tick)