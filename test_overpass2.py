import urllib.request, urllib.parse, json

# Test Overpass API for Toowoomba area (high pop, first PASS opportunity)
query = """[out:json][timeout:25];
(
  node["amenity"="pharmacy"](around:15000,-27.5598,151.9507);
  way["amenity"="pharmacy"](around:15000,-27.5598,151.9507);
);
out center;"""

url = 'https://overpass-api.de/api/interpreter?data=' + urllib.parse.quote(query)
resp = urllib.request.urlopen(url)
data = json.loads(resp.read())
elements = data.get('elements', [])
print(f'Found {len(elements)} pharmacies near Toowoomba')
for e in elements[:10]:
    name = e.get('tags', {}).get('name', 'Unknown')
    lat = e.get('lat', e.get('center', {}).get('lat', 0))
    lng = e.get('lon', e.get('center', {}).get('lon', 0))
    print(f'  {name} @ {lat},{lng}')
if len(elements) > 10:
    print(f'  ... and {len(elements)-10} more')
