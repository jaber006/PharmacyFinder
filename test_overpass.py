import urllib.request, urllib.parse, json

# Test Overpass API for Coonamble area
query = """[out:json][timeout:25];
(
  node["amenity"="pharmacy"](around:15000,-30.9553,148.3936);
  way["amenity"="pharmacy"](around:15000,-30.9553,148.3936);
);
out center;"""

url = 'https://overpass-api.de/api/interpreter?data=' + urllib.parse.quote(query)
resp = urllib.request.urlopen(url)
data = json.loads(resp.read())
elements = data.get('elements', [])
print(f'Found {len(elements)} pharmacies near Coonamble')
for e in elements:
    name = e.get('tags', {}).get('name', 'Unknown')
    lat = e.get('lat', e.get('center', {}).get('lat', 0))
    lng = e.get('lon', e.get('center', {}).get('lon', 0))
    print(f'  {name} @ {lat},{lng}')
