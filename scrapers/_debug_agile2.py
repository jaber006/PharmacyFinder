import re
import requests
from bs4 import BeautifulSoup

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
})

url = "https://www.agilebb.com.au/pharmacies-for-sale"
resp = session.get(url, timeout=20)
soup = BeautifulSoup(resp.text, 'html.parser')
for tag in soup(['script', 'style']):
    tag.decompose()
text = soup.get_text(separator='\n', strip=True)

# Find lines with "Ref" 
for line in text.split('\n'):
    if 'Ref' in line or 'ref' in line.lower():
        # Show hex of first 30 chars
        print(f"LINE: {repr(line[:120])}")

print("\n--- Full text around LN ---")
idx = text.find('LN')
if idx > 0:
    print(repr(text[idx-30:idx+100]))
else:
    print("LN not found in text!")
    # Try in HTML
    if 'LN' in resp.text:
        idx = resp.text.find('Ref LN')
        if idx > 0:
            print(f"Found in HTML at {idx}: {resp.text[idx:idx+100]}")
        else:
            print("Ref LN not in HTML either")
    else:
        print("LN not in HTML at all")
