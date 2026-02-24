"""Test extracting structured pharmacy data from Google Maps."""
import sys, json, time, re, os
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)

from playwright.sync_api import sync_playwright

lat, lng = -30.9553, 148.3936  # Coonamble

def extract_coords_from_href(href):
    """Extract lat/lng from Google Maps place URL."""
    # Pattern: !3d{lat}!4d{lng} in the data parameter
    lat_match = re.search(r'!3d(-?[\d.]+)', href)
    lng_match = re.search(r'!4d(-?[\d.]+)', href)
    if lat_match and lng_match:
        return float(lat_match.group(1)), float(lng_match.group(1))
    # Pattern: @{lat},{lng} in URL
    at_match = re.search(r'@(-?[\d.]+),(-?[\d.]+)', href)
    if at_match:
        return float(at_match.group(1)), float(at_match.group(2))
    return None, None

def extract_name_from_href(href):
    """Extract place name from URL."""
    m = re.search(r'/maps/place/([^/]+)', href)
    if m:
        return m.group(1).replace('+', ' ')
    return None

def scrape_pharmacies(page, search_lat, search_lng, zoom=13):
    """Scrape pharmacy results from the current Google Maps page."""
    url = f"https://www.google.com/maps/search/pharmacy/@{search_lat},{search_lng},{zoom}z"
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    time.sleep(4)
    
    # Handle consent if needed
    try:
        accept = page.locator("button:has-text('Accept all')").first
        if accept.is_visible(timeout=2000):
            accept.click()
            time.sleep(2)
    except:
        pass
    
    # Wait for results to load
    time.sleep(3)
    
    results = []
    
    # Get place links with coordinates
    links = page.locator('a[href*="/maps/place/"]').all()
    
    for link in links:
        try:
            href = link.get_attribute('href') or ''
            p_lat, p_lng = extract_coords_from_href(href)
            name = extract_name_from_href(href)
            
            # Try to get the parent result card's text for address/rating
            parent = link.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
            card_text = ''
            try:
                card_text = parent.inner_text(timeout=2000)
            except:
                pass
            
            # Extract rating from card text
            rating = None
            rating_match = re.search(r'(\d+\.?\d*)\s*(?:star|★)', card_text, re.IGNORECASE)
            if not rating_match:
                # Look for pattern like "4.5(123)"
                rating_match = re.search(r'^(\d+\.?\d*)\s*\(', card_text, re.MULTILINE)
            if rating_match:
                rating = float(rating_match.group(1))
            
            # Extract address - usually after "Pharmacy · " line
            address = ''
            addr_match = re.search(r'Pharmacy\s*[·•]\s*[♿🦽]*\s*[·•]?\s*(.+?)(?:\n|Open|Closed|Opens|No reviews)', card_text, re.DOTALL)
            if addr_match:
                address = addr_match.group(1).strip()
            
            results.append({
                'name': name or 'Unknown',
                'lat': p_lat,
                'lng': p_lng,
                'rating': rating,
                'address': address,
                'raw_text': card_text[:200]
            })
        except Exception as e:
            print(f"  Error processing link: {e}")
    
    return results


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        viewport={"width": 1280, "height": 900},
        locale="en-AU",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    page = ctx.new_page()
    
    print("Testing Coonamble...")
    results = scrape_pharmacies(page, lat, lng)
    print(f"Found {len(results)} pharmacies:")
    for r in results:
        print(f"  {r['name']} @ {r['lat']}, {r['lng']}")
        print(f"    Rating: {r['rating']}, Address: {r['address']}")
        print(f"    Raw: {r['raw_text'][:150]}")
    
    # Test with a higher-population area
    print("\nTesting Toowoomba...")
    results2 = scrape_pharmacies(page, -27.5598, 151.9507)
    print(f"Found {len(results2)} pharmacies:")
    for r in results2:
        print(f"  {r['name']} @ {r['lat']}, {r['lng']}")
    
    # Test with wider zoom for 15km coverage
    print("\nTesting Toowoomba zoom=11 (wider)...")
    results3 = scrape_pharmacies(page, -27.5598, 151.9507, zoom=11)
    print(f"Found {len(results3)} pharmacies:")
    for r in results3:
        print(f"  {r['name']} @ {r['lat']}, {r['lng']}")
    
    browser.close()
