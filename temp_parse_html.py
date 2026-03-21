import re, json
content = open('temp_cre_test.html', 'r', encoding='utf-8').read()
print(f"HTML length: {len(content)}")

# Find property listing links
links = re.findall(r'href="([^"]*?property-[^"]*?)"', content)
print(f"\nProperty links: {len(links)}")
for l in links[:5]:
    print(f"  {l[:120]}")

# Find data- attributes
data_attrs = re.findall(r'data-listing-id="([^"]*)"', content)
print(f"\ndata-listing-id: {len(data_attrs)}")

# Find JSON-LD
jsonld = re.findall(r'<script type="application/ld\+json">(.*?)</script>', content, re.DOTALL)
print(f"\nJSON-LD blocks: {len(jsonld)}")
for j in jsonld[:2]:
    try:
        d = json.loads(j)
        print(f"  type: {d.get('@type', 'unknown')}")
    except:
        print(f"  (parse error)")

# Look for window.__data or similar
data_patterns = [
    r'window\.__\w+\s*=\s*({.*?});\s*</script>',
    r'window\.initialState\s*=\s*({.*?});\s*</script>',
    r'"listingId"\s*:\s*"?(\d+)"?',
    r'"address"\s*:\s*"([^"]+)"',
]
for pat in data_patterns:
    matches = re.findall(pat, content[:200000], re.DOTALL)
    if matches:
        print(f"\n  Pattern {pat[:40]}: {len(matches)} matches")
        for m in matches[:3]:
            print(f"    {str(m)[:120]}")

# Check for specific class names
class_patterns = ['listing', 'property', 'card', 'result', 'address', 'price']
for cp in class_patterns:
    count = content.lower().count(f'class="{cp}') + content.lower().count(f"class='{cp}") + content.lower().count(f'class="{cp}')
    classes = re.findall(rf'class="([^"]*{cp}[^"]*)"', content, re.IGNORECASE)
    unique_classes = set(c for c in classes)
    if unique_classes:
        print(f"\n  Classes containing '{cp}':")
        for uc in sorted(unique_classes)[:5]:
            print(f"    {uc[:100]}")
